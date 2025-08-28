# =================
# app.py — Streamlit + Neon (Postgres) (+ Cloudflare R2 optionnel)
# Optimisations : 1 seule requête SQL, cache, filtre catégories, pagination
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

# --- (optionnel) R2
try:
    import boto3
    from botocore.client import Config
except Exception:
    boto3 = None
    Config = None

# --- Config de la page
st.set_page_config(
    page_title="Catalogue Produits — TechSell × LG",
    page_icon="https://raw.githubusercontent.com/fabienbonnet0405-jpg/appli-photos-lg/main/LG%20LOGO.png",
    layout="wide",
)

# --- Logo en header
st.image(
    "https://raw.githubusercontent.com/fabienbonnet0405-jpg/appli-photos-lg/main/Logo%20Techsell.jpg",
    width=200
)
st.title("🛒 Catalogue Produits — TechSell")

# --- Gate par mot de passe (secret APP_PASSWORD à définir sur Streamlit Cloud)
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

# --- Charger secrets dans env si besoin (utile en Cloud)
try:
    if hasattr(st, "secrets") and "NEON_DATABASE_URL" not in os.environ:
        for k, v in st.secrets.items():
            os.environ.setdefault(k, str(v))
except Exception:
    pass

# --- Env vars
NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")  # ex: postgresql+pg8000://user:pass@host/neondb
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET", "photos")

if not NEON_DATABASE_URL:
    st.error("⚠️ NEON_DATABASE_URL manquante. Renseigne-la dans Advanced settings → Secrets.")
    st.stop()

# --- SQLAlchemy engine (pg8000 + SSL)
ssl_ctx = ssl.create_default_context()
engine: Engine = create_engine(
    NEON_DATABASE_URL,                   # format: postgresql+pg8000://...
    connect_args={"ssl_context": ssl_ctx},
    pool_pre_ping=True,
)

# --- Client Cloudflare R2 (optionnel)
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

# --- Schéma minimal
SCHEMA_SQL = """
create extension if not exists pgcrypto;

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

alter table if exists products add column if not exists photo_url text;
alter table if exists products add column if not exists price numeric(10,2);
alter table if exists products add column if not exists cost  numeric(10,4);
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

# --- Auth minimal (email / rôle)
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

# --- Header compact LG (propre et centré à gauche)
LOGO_LG = "https://raw.githubusercontent.com/fabienbonnet0405-jpg/appli-photos-lg/main/LG%20LOGO.png"

st.markdown(
    f"""
    <style>
      .app-header {{
        display: flex; align-items: center; gap: 14px;
        margin: 4px 0 18px;
      }}
      .app-header img {{
        height: 40px; width: auto; border-radius: 4px;
      }}
      .app-title {{
        font-size: 26px; font-weight: 700; margin: 0; line-height: 1.2;
      }}
      .app-sub {{
        font-size: 14px; color: #6b7280; margin: 0; line-height: 1.2;
      }}
    </style>
    <div class="app-header">
      <img src="{LOGO_LG}" alt="LG">
      <div>
        <p class="app-title">Catalogue Produits</p>
        <p class="app-sub">TechSell × LG</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)
list_tab, import_tab, photo_tab = st.tabs(["Liste produits", "Admin · Import Excel", "Photos"])

# ======================
# Loader optimisé : 1 seule requête + cache (60s)
# ======================
@st.cache_data(ttl=60)
# --- Optimisation : récupérer tout en une requête
@st.cache_data(ttl=60)
def load_products(q: str, sel_cats):
    """Retourne produits avec price/cost stockés directement dans products."""
    where_sql = """
      where (:q = '' 
         or lower(p.name) like lower('%' || :q || '%') 
         or lower(p.sku)  like lower('%' || :q || '%'))
    """
    params = {"q": q or ""}

    if sel_cats:
        placeholders = ", ".join([f":c{i}" for i in range(len(sel_cats))])
        where_sql += f" and p.category in ({placeholders})"
        for i, c in enumerate(sel_cats):
            params[f"c{i}"] = c

    sql = f"""
    select
      p.id, p.sku, p.name, p.brand, p.category, p.photo_url,
      p.price, p.cost
    from products p
    {where_sql}
    order by p.name asc
    limit 1000
    """
    return fetch_all(sql, **params)

# ======================
# Liste produits
# ======================
with list_tab:
    col1, col2 = st.columns([2, 1])
    q = col1.text_input("Recherche", placeholder="Nom ou SKU…")
    store_code = col2.text_input("Code magasin (optionnel)")

    # Filtre catégories (via base)
    all_cats = [r["category"] for r in fetch_all(
        "select distinct category from products where category is not null order by 1"
    )]
    sel_cats = st.multiselect("Catégories", options=all_cats, default=[])

    # Résolution store_id une seule fois
    store = None
    if store_code:
        res = fetch_all("select id, code from stores where code = :code limit 1", code=store_code)
        store = res[0] if res else None

    # Chargement optimisé (cache 60s)
    rows = load_products(q, sel_cats)

    # Pagination
    top = st.columns([1,1,2,2])
    with top[0]:
        page_size = st.selectbox("Cartes/page", [6, 12, 24, 48], index=1)
    with top[1]:
        page = st.number_input("Page", min_value=1, value=1, step=1)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    st.caption(f"{len(rows)} produit(s) — page {page} ({len(page_rows)} affichés)")
    cols = st.columns(3)

    for i, r in enumerate(page_rows):
        with cols[i % 3]:
            # IMAGE (photo_url prioritaire / placeholder sinon)
            img_src = r.get("photo_url") or "https://placehold.co/600x400?text=Photo"
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

            # TITRE / SKU
            st.subheader(r["name"])
            st.caption(f"SKU: {r['sku']} · {r.get('brand') or ''}")

            # PV / COUT / MARGE / COEFF (déjà chargés)
            pv = float(r["price"]) if r.get("price") is not None else None
            c  = float(r["cost"])  if r.get("cost")  is not None else None

            if pv is None:
                st.markdown("*Pas de prix actif*")
            else:
                if c is not None:
                    m_eur = pv - c
                    m_pct = (m_eur / pv * 100) if pv else 0
                    coeff = (pv / c) if c else 0
                    badge = "🟢" if m_pct >= 20 else ("🟠" if m_pct >= 10 else "🔴")
                    coeff_badge = "🟢" if coeff >= 1.30 else ("🟠" if coeff >= 1.20 else "🔴")
                    st.markdown(
                        f"**PV**: {pv:.2f} € · **Coût**: {c:.2f} € · "
                        f"**Marge**: {m_eur:.2f} € ({m_pct:.0f}%) {badge} · "
                        f"**Coeff**: {coeff:.2f} {coeff_badge}"
                    )
                else:
                    st.markdown(f"**PV**: {pv:.2f} € · *Coût inconnu*")

# ======================
# Admin · Import Excel (réservé admin)
# ======================
with import_tab:
    if user["role"] != "admin":
        st.warning("Réservé aux administrateurs.")
    else:
        st.subheader("Importer un fichier Excel (.xlsx)")
        st.markdown("Colonnes attendues : sku, name, category, cost, price, photo_url")

        # Purge
        if st.button("⚠️ Purger tous les produits AVANT d'importer"):
            execute("delete from products")
            st.success("🧹 Base vidée. Tu peux importer ton Excel proprement.")
            st.cache_data.clear()

        up = st.file_uploader("Dépose ton Excel (1 onglet 'catalogue')", type=["xlsx"])
        if up and st.button("Importer"):
            try:
                data = up.read()
                wb = pd.ExcelFile(io.BytesIO(data))

                if "catalogue" not in wb.sheet_names:
                    st.error("L'Excel doit contenir un onglet 'catalogue' avec les colonnes : sku, name, category, cost, price, photo_url.")
                    st.stop()

                df = wb.parse("catalogue").fillna("")
                # Nettoyage / typage
                for col in ["sku","name","category","photo_url"]:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip()
                if "cost" in df.columns:
                    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")
                if "price" in df.columns:
                    df["price"] = pd.to_numeric(df["price"], errors="coerce")

                # Upsert direct dans products (modèle simple)
                for _, r in df.iterrows():
                    execute(
                        """
                        insert into products (id, sku, name, category, status, photo_url, price, cost)
                        values (:id, :sku, :name, :category, 'active', :photo_url, :price, :cost)
                        on conflict (sku) do update set
                          name=excluded.name,
                          category=excluded.category,
                          status='active',
                          photo_url=coalesce(nullif(excluded.photo_url, ''), products.photo_url),
                          price=excluded.price,
                          cost=excluded.cost
                        """,
                        id=str(uuid.uuid4()),
                        sku=r["sku"],
                        name=r["name"],
                        category=(r["category"] or None),
                        photo_url=(r["photo_url"] or None),
                        price=(float(r["price"]) if pd.notna(r["price"]) else None),
                        cost=(float(r["cost"]) if pd.notna(r["cost"]) else None),
                    )

                st.cache_data.clear()
                st.success("✅ Import terminé (modèle simple).")
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
