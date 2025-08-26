# =================
# app.py  — Streamlit + Neon (Postgres) + (optionnel) Cloudflare R2
# (aucune dépendance à Supabase)
# =================
import os
import io
import uuid
import ssl
import datetime as dt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Boto3 (optionnel pour R2)
try:
    import boto3
    from botocore.client import Config
except Exception:
    boto3 = None
    Config = None

# --- Config de la page
st.set_page_config(page_title="Produits & Rentabilité", page_icon="📦", layout="wide")

# ---- Gate d'accès par mot de passe (optionnel)
APP_PWD = None
try:
    if hasattr(st, "secrets"):
        APP_PWD = st.secrets.get("APP_PASSWORD")
except Exception:
    pass

if APP_PWD:
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False
    if not st.session_state.auth_ok:
        pwd_in = st.text_input("Mot de passe d'accès (test)", type="password")
        if st.button("Entrer"):
            st.session_state.auth_ok = (pwd_in == APP_PWD)
            if not st.session_state.auth_ok:
                st.error("Mot de passe incorrect.")
        st.stop()


# --- Charger les secrets Streamlit dans les env vars (utile en Cloud)
try:
    if hasattr(st, "secrets") and "NEON_DATABASE_URL" not in os.environ:
        for k, v in st.secrets.items():
            os.environ.setdefault(k, str(v))
except Exception:
    pass

# --- Récupération des variables d'env
NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")  # ex: postgresql+pg8000://user:pass@host/neondb
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET", "photos")

if not NEON_DATABASE_URL:
    st.error("⚠️ NEON_DATABASE_URL manquante. Renseigne-la via $env:NEON_DATABASE_URL ou .streamlit/secrets.toml")
    st.stop()

# --- IMPORTANT: URL sans paramètres SSL ; on fournit le contexte SSL au driver pg8000 via connect_args
# Exemple d’URL:
#   postgresql+pg8000://neondb_owner:TON_MDP@ep-xxxxx.eu-central-1.aws.neon.tech/neondb
ssl_ctx = ssl.create_default_context()
engine: Engine = create_engine(
    NEON_DATABASE_URL,                   # doit être en pg8000
    connect_args={"ssl_context": ssl_ctx},  # ✅ pg8000 attend ssl_context (pas "ssl=True")
    pool_pre_ping=True,
)

# --- Client Cloudflare R2 (facultatif)
r2 = None
if R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and boto3:
    r2 = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

# --- Schéma minimal (création si absent)
SCHEMA_SQL = """
create extension if not exists pgcrypto; -- pour gen_random_uuid();

create table if not exists products (
  id uuid primary key default gen_random_uuid(),
  sku text unique not null,
  name text not null,
  brand text,
  category text,
  status text default 'active'
);

create table if not exists stores (
  id uuid primary key default gen_random_uuid(),
  code text unique not null,
  name text not null,
  sector_id uuid
);

create table if not exists prices (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references products(id) on delete cascade,
  store_id uuid references stores(id) on delete set null,
  price numeric(10,2) not null,
  valid_from date not null,
  valid_to date
);

create table if not exists costs (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references products(id) on delete cascade,
  cost numeric(10,4) not null,
  valid_from date not null,
  valid_to date
);

create table if not exists photos (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references products(id) on delete cascade,
  key text not null,
  taken_by text,
  taken_at timestamptz default now()
);

-- Ajoute la colonne photo_url si elle n'existe pas
alter table if exists products add column if not exists photo_url text;
"""
with engine.begin() as conn:
    conn.exec_driver_sql(SCHEMA_SQL)

# --- Helpers DB
def fetch_all(sql: str, **params):
    with engine.begin() as conn:
        res = conn.execute(text(sql), params).mappings().all()
        return [dict(r) for r in res]

def execute(sql: str, **params):
    with engine.begin() as conn:
        conn.execute(text(sql), params)

# --- Auth minimal
if "user" not in st.session_state:
    st.session_state.user = None

with st.sidebar:
    st.markdown("## Connexion")
    email = st.text_input("Email professionnel", placeholder="prenom.nom@entreprise.com")
    role = st.selectbox("Rôle", ["chef", "admin", "viewer"], index=0)
    if st.button("Se connecter"):
        st.session_state.user = {"email": (email or "").strip(), "role": role}
    if st.button("Se déconnecter"):
        st.session_state.user = None

user = st.session_state.user
if not user:
    st.info("🔐 Connecte-toi dans la barre latérale pour continuer.")
    st.stop()

# --- UI
st.title("📦 Produits, Prix & Rentabilité — Neon (+ R2 optionnel)")
list_tab, import_tab, photo_tab = st.tabs(["Liste produits", "Admin · Import Excel", "Photos"])

# ======================
# Liste produits
with list_tab:
    col1, col2 = st.columns([2, 1])
    q = col1.text_input("Recherche", placeholder="Nom ou SKU…")
    store_code = col2.text_input("Code magasin (optionnel)")
    # --- Filtre catégorie
    all_cats = [r["category"] for r in fetch_all("select distinct category from products where category is not null order by 1")]
    sel_cats = st.multiselect("Catégories", options=all_cats, default=[])

    # Construction dynamique du WHERE (nom/SKU + catégories)
    where_sql = """
        where (:q = '' 
           or lower(name) like lower('%' || :q || '%') 
           or lower(sku) like lower('%' || :q || '%'))
    """
    params = {"q": q or ""}

    if sel_cats:
        # on fabrique un IN (:c0,:c1,...) pour éviter les soucis d'array binding
        placeholders = ", ".join([f":c{i}" for i in range(len(sel_cats))])
        where_sql += f" and category in ({placeholders})"
        for i, c in enumerate(sel_cats):
            params[f"c{i}"] = c

    rows = fetch_all(
        f"""
        select id, sku, name, brand, category 
        from products
        {where_sql}
        order by name asc
        limit 500
        """,
        **params,
    )

    st.caption(f"{len(rows)} produit(s)")
    cols = st.columns(3)

    store = None
    if store_code:
        res = fetch_all("select id, code from stores where code = :code limit 1", code=store_code)
        store = res[0] if res else None

    for i, r in enumerate(rows):
        with cols[i % 3]:
            # --- Récup photo produit
            photo_url_row = fetch_all("select photo_url from products where id=:pid", pid=r["id"])
            img_src = photo_url_row[0]["photo_url"] if photo_url_row and photo_url_row[0]["photo_url"] else "https://placehold.co/600x400?text=Photo"

            # --- Vignette uniforme
            st.markdown(
                f'''
                <div style="
                    width:100%;
                    aspect-ratio: 4 / 3;
                    background:#fff;border:1px solid #eee;border-radius:12px;
                    overflow:hidden;display:flex;align-items:center;justify-content:center;
                ">
                  <img src="{img_src}" style="max-width:100%; max-height:100%;" />
                </div>
                ''',
                unsafe_allow_html=True
            )

            # --- Titre + SKU (une seule fois !)
            st.subheader(r["name"])
            st.caption(f"SKU: {r['sku']} · {r.get('brand') or ''}")

            # --- Prix / coût / marge
            today = dt.date.today()
            price_sql = """
                select price from prices
                where product_id=:pid
                  and valid_from <= :today
                  and (valid_to is null or valid_to >= :today)
            """
            price_params = {"pid": r["id"], "today": today}
            if store:
                price_sql += " and store_id = :sid"
                price_params["sid"] = store["id"]
            price_sql += " order by valid_from desc limit 1"
            price = fetch_all(price_sql, **price_params)

            cost = fetch_all(
                """
                select cost from costs
                where product_id=:pid
                  and valid_from <= :today
                  and (valid_to is null or valid_to >= :today)
                order by valid_from desc
                limit 1
                """,
                pid=r["id"], today=today,
            )

            if price:
                pv = float(price[0]["price"])
                if cost:
                    c = float(cost[0]["cost"])
                    m_eur = pv - c
                    m_pct = (m_eur / pv * 100) if pv else 0
                    coeff = pv / c if c else 0
                    badge = "🟢" if m_pct >= 20 else ("🟠" if m_pct >= 10 else "🔴")
                    st.markdown(
                        f"**PV**: {pv:.2f} € · **Coût**: {c:.2f} € · "
                        f"**Marge**: {m_eur:.2f} € ({m_pct:.0f}%) · "
                        f"**Coeff**: {coeff:.2f} {badge}"
                    )
                else:
                    st.markdown(f"**PV**: {pv:.2f} €")
            else:
                st.markdown("*Pas de prix actif*")

            # --- fin carte
            st.markdown("</div>", unsafe_allow_html=True)

# ======================
# Import Excel (admin uniquement)
# ======================
with import_tab:
    if user["role"] != "admin":
        st.warning("Réservé aux administrateurs.")
    else:
        st.subheader("Importer un fichier Excel (.xlsx)")
        st.markdown(
            "Colonnes attendues : **products**(sku,name,brand,category,status,photo_url) · "
            "**stores**(code,name,sector_id) · **prices**(sku,store_code,price,valid_from,valid_to) · "
            "**costs**(sku,cost,valid_from,valid_to)"
        )

        # -------- BTN PURGE AVANT IMPORT --------
        if st.button("⚠️ Purger tous les produits/prix/coûts/photos AVANT d'importer"):
            execute("delete from photos")
            execute("delete from prices")
            execute("delete from costs")
            execute("delete from products")
            execute("delete from stores")
            st.success("🧹 Base vidée. Tu peux importer ton Excel proprement.")
        # ----------------------------------------

        up = st.file_uploader("Dépose ton Excel", type=["xlsx"])

        if up and st.button("Importer"):
            try:
                data = up.read()
                wb = pd.ExcelFile(io.BytesIO(data))

                # ---- Upserts
                def upsert_products(df: pd.DataFrame):
                    has_url = "photo_url" in df.columns
                    for _, r in df.iterrows():
                        execute(
                            """
                            insert into products (id, sku, name, brand, category, status, photo_url)
                            values (:id, :sku, :name, :brand, :category, coalesce(:status,'active'), :photo_url)
                            on conflict (sku) do update set
                              name=excluded.name,
                              brand=excluded.brand,
                              category=excluded.category,
                              status=excluded.status,
                              photo_url=coalesce(excluded.photo_url, products.photo_url)
                            """,
                            id=str(uuid.uuid4()),
                            sku=str(r["sku"]).strip(),
                            name=str(r["name"]).strip(),
                            brand=(r.get("brand") or None),
                            category=(r.get("category") or None),
                            status=(r.get("status") or "active"),
                            photo_url=(str(r["photo_url"]).strip() if has_url and pd.notna(r.get("photo_url")) and str(r["photo_url"]).strip() else None),
                        )

                def upsert_stores(df: pd.DataFrame):
                    for _, r in df.iterrows():
                        execute(
                            """
                            insert into stores (id, code, name, sector_id)
                            values (:id, :code, :name, :sector)
                            on conflict (code) do update set name=excluded.name, sector_id=excluded.sector_id
                            """,
                            id=str(uuid.uuid4()),
                            code=str(r["code"]).strip(),
                            name=str(r["name"]).strip(),
                            sector=(r.get("sector_id") or None),
                        )

                def get_product_id(sku: str):
                    res = fetch_all("select id from products where sku=:sku limit 1", sku=sku)
                    return res[0]["id"] if res else None

                def get_store_id(code: str):
                    res = fetch_all("select id from stores where code=:code limit 1", code=code)
                    return res[0]["id"] if res else None

                # ---- Lecture des onglets
                if "products" in wb.sheet_names:
                    upsert_products(wb.parse("products").fillna(""))
                if "stores" in wb.sheet_names:
                    upsert_stores(wb.parse("stores").fillna(""))

                if "prices" in wb.sheet_names:
                    dfp = wb.parse("prices").dropna(subset=["sku", "price", "valid_from"]).fillna("")
                    for _, r in dfp.iterrows():
                        pid = get_product_id(str(r["sku"]).strip())
                        sid = get_store_id(str(r.get("store_code", "")).strip()) if r.get("store_code", "") else None
                        if not pid:
                            continue
                        execute(
                            """
                            insert into prices (id, product_id, store_id, price, valid_from, valid_to)
                            values (:id, :pid, :sid, :price, :vf, :vt)
                            on conflict do nothing
                            """,
                            id=str(uuid.uuid4()),
                            pid=pid,
                            sid=sid,
                            price=float(r["price"]),
                            vf=pd.to_datetime(r["valid_from"]).date(),
                            vt=(pd.to_datetime(r.get("valid_to")).date() if str(r.get("valid_to", "")) else None),
                        )

                if "costs" in wb.sheet_names:
                    dfc = wb.parse("costs").dropna(subset=["sku", "cost", "valid_from"]).fillna("")
                    for _, r in dfc.iterrows():
                        pid = get_product_id(str(r["sku"]).strip())
                        if not pid:
                            continue
                        execute(
                            """
                            insert into costs (id, product_id, cost, valid_from, valid_to)
                            values (:id, :pid, :cost, :vf, :vt)
                            on conflict do nothing
                            """,
                            id=str(uuid.uuid4()),
                            pid=pid,
                            cost=float(r["cost"]),
                            vf=pd.to_datetime(r["valid_from"]).date(),
                            vt=(pd.to_datetime(r.get("valid_to")).date() if str(r.get("valid_to", "")) else None),
                        )

                st.success("✅ Import terminé.")
            except Exception as e:
                st.exception(e)

# ======================
# Photos (upload + galerie) — optionnel, nécessite R2
# ======================
with photo_tab:
    if not r2:
        st.warning("Configure R2 (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY) pour activer l'upload.")
    st.subheader("Uploader une photo produit")
    sku = st.text_input("SKU produit")
    file = st.file_uploader("Photo (jpg/png)", type=["jpg", "jpeg", "png"])
    if st.button("Envoyer la photo"):
        if not sku or not file or not r2:
            st.error("SKU/fichier manquant ou R2 non configuré.")
        else:
            pr = fetch_all("select id from products where sku=:sku limit 1", sku=sku)
            if not pr:
                st.error("SKU inconnu.")
            else:
                pid = pr[0]["id"]
                ext = os.path.splitext(file.name)[1].lower() or ".jpg"
                key = f"{pid}/{uuid.uuid4()}{ext}"
                try:
                    r2.upload_fileobj(file, R2_BUCKET, key)
                    execute(
                        "insert into photos (id, product_id, key, taken_by, taken_at) values (:id,:pid,:key,:by,:at)",
                        id=str(uuid.uuid4()), pid=pid, key=key, by=user["email"], at=dt.datetime.utcnow(),
                    )
                    st.success("📸 Photo enregistrée.")
                except Exception as e:
                    st.exception(e)

    st.divider()
    st.subheader("Galerie par SKU")
    sku_g = st.text_input("SKU pour la galerie", key="gal_r2")
    if sku_g and r2:
        pr = fetch_all("select id from products where sku=:sku limit 1", sku=sku_g)
        if pr:
            pid = pr[0]["id"]
            phs = fetch_all("select key, taken_at from photos where product_id=:pid order by taken_at desc limit 30", pid=pid)
            cols = st.columns(4)
            for i, ph in enumerate(phs):
                try:
                    url = r2.generate_presigned_url(
                        ClientMethod="get_object",
                        Params={"Bucket": R2_BUCKET, "Key": ph["key"]},
                        ExpiresIn=300,
                    )
                    with cols[i % 4]:
                        st.image(url, use_container_width=True)
                        st.caption(str(ph["taken_at"]))
                except Exception:
                    pass
