# Databricks notebook source
# MAGIC %md
# MAGIC # Nordic Snacks: generátor cvičných dat
# MAGIC
# MAGIC Všechna data jsou vymyšlená. Notebook vytvoří zdroje, které odpovídají OFCaS:
# MAGIC
# MAGIC | Zdroj | Kde vznikne | Obdoba v OFCaS |
# MAGIC |---|---|---|
# MAGIC | `ofc_sap.customers / products / sales_orders` | Delta tabulky (jen ke čtení) | SAP data od OF Central |
# MAGIC | `landing/sharepoint/...` | CSV + Excel | ručně udržované soubory ze SharePointu |
# MAGIC | `landing/promo/promo_snapshots/` | denní full snapshot CSV | Limigo |
# MAGIC | `landing/market/CZ|SK/` | měsíční CSV | Nielsen |
# MAGIC
# MAGIC **Scénáře** (widget `scenario`):
# MAGIC - `initial`: první dodávka, vytvoří vše od nuly (smaže a znovu vytvoří zdroje)
# MAGIC - `delivery2`: druhá dodávka, simuluje změny v čase (spouštěj až po `initial` a po načtení do bronze/silver)
# MAGIC
# MAGIC Spouštěj na Databricks computu (serverless), ne lokálně. Zapisuje do `/Volumes/...`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("scenario", "initial", ["initial", "delivery2"], "Scénář")
dbutils.widgets.text("seed", "42", "Seed")

catalog = dbutils.widgets.get("catalog")
scenario = dbutils.widgets.get("scenario")
seed = int(dbutils.widgets.get("seed"))

LANDING = f"/Volumes/{catalog}/integration/landing"
print(f"catalog={catalog}, scenario={scenario}, seed={seed}")
print(f"landing={LANDING}")

# COMMAND ----------

import os
import random
import shutil
from datetime import date, timedelta

import pandas as pd
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ofc_sap")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.integration")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.integration.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Master data (pevně dané, bez náhody)

# COMMAND ----------

cz_names = ["Praha Market s.r.o.", "Brno Shop a.s.", "Ostrava Potraviny s.r.o.", "Plzeň Retail a.s.",
            "Liberec Diskont s.r.o.", "Olomouc Trade s.r.o.", "Hradec Fresh a.s.", "Pardubice Food s.r.o.",
            "Zlín Obchod s.r.o.", "Jihlava Market a.s."]
cz_cities = ["Praha", "Brno", "Ostrava", "Plzeň", "Liberec", "Olomouc", "Hradec Králové", "Pardubice", "Zlín", "Jihlava"]
sk_names = ["Bratislava Market s.r.o.", "Košice Retail a.s.", "Žilina Potraviny s.r.o.", "Nitra Diskont s.r.o.",
            "Banská Fresh a.s.", "Prešov Trade s.r.o.", "Trnava Obchod s.r.o.", "Trenčín Food a.s.",
            "Martin Market s.r.o.", "Poprad Shop s.r.o."]
sk_cities = ["Bratislava", "Košice", "Žilina", "Nitra", "Banská Bystrica", "Prešov", "Trnava", "Trenčín", "Martin", "Poprad"]
SEGMENTS = ["HYP", "SUP", "DIS", "CON"]

customers = []
for i, (n, c) in enumerate(zip(cz_names, cz_cities)):
    customers.append(dict(customer_id=f"{100001 + i:010d}", name=n, reg_no=f"{25000000 + i * 13579:08d}",
                          city=c, country="CZ", segment_code=SEGMENTS[i % 4], changed_at="2026-01-15 08:00:00"))
for j, (n, c) in enumerate(zip(sk_names, sk_cities)):
    customers.append(dict(customer_id=f"{100101 + j:010d}", name=n, reg_no=f"{36000000 + j * 24681:08d}",
                          city=c, country="SK", segment_code=SEGMENTS[j % 4], changed_at="2026-01-15 08:00:00"))

# Zákazníci, kteří se v prodejích objeví dřív, než dorazí master data (inferred member)
UNKNOWN_IDS = ["0000199901", "0000199902"]

products = [
    ("P001", "Bramborové chipsy solené 150g", "Chipsy", 39.90),
    ("P002", "Bramborové chipsy paprika 150g", "Chipsy", 39.90),
    ("P003", "Chipsy sýr a cibule 150g", "Chipsy", 41.90),
    ("P004", "Křupky kukuřičné 100g", "Křupky", 29.90),
    ("P005", "Křupky arašídové 100g", "Křupky", 32.90),
    ("P006", "Křupky pizza 100g", "Křupky", 31.90),
    ("P007", "Arašídy pražené solené 200g", "Ořechy", 59.90),
    ("P008", "Mandle pražené 150g", "Ořechy", 89.90),
    ("P009", "Směs ořechů 250g", "Ořechy", 99.90),
    ("P010", "Müsli tyčinka čokoláda 40g", "Tyčinky", 19.90),
    ("P011", "Müsli tyčinka ovoce 40g", "Tyčinky", 19.90),
    ("P012", "Proteinová tyčinka 60g", "Tyčinky", 34.90),
]

cust_country = {c["customer_id"]: c["country"] for c in customers}
cust_country.update({UNKNOWN_IDS[0]: "CZ", UNKNOWN_IDS[1]: "SK"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pomocné funkce

# COMMAND ----------

def prep(pdf, date_cols=(), ts_cols=(), dec_cols=()):
    """pandas -> Spark DataFrame se správnými typy."""
    sdf = spark.createDataFrame(pdf)
    for c in date_cols:
        sdf = sdf.withColumn(c, F.to_date(c))
    for c in ts_cols:
        sdf = sdf.withColumn(c, F.to_timestamp(c))
    for c in dec_cols:
        sdf = sdf.withColumn(c, F.col(c).cast("decimal(14,2)"))
    return sdf


def save_table(pdf, name, **kw):
    prep(pdf, **kw).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog}.ofc_sap.{name}")


def upsert(pdf, name, keys, **kw):
    """MERGE do existující tabulky, takže opakované spuštění nevytvoří duplicity."""
    prep(pdf, **kw).createOrReplaceTempView("_upd")
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""MERGE INTO {catalog}.ofc_sap.{name} t USING _upd s ON {cond}
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def write_csv(pdf, path, **kw):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pdf.to_csv(path, index=False, encoding="utf-8", **kw)
    print("  zapsáno:", path.replace(LANDING + "/", ""))


def gen_orders(rng, n_orders, d_from, d_to, id_start, customer_ids, unknown_ids=()):
    """Prodejní objednávky: 1-3 řádky, ~3 % jsou vratky (záporné hodnoty, order_type RE)."""
    days = (d_to - d_from).days
    rows = []
    for k in range(n_orders):
        oid = f"{4500000000 + id_start + k:010d}"
        if unknown_ids and k % 40 == 7:
            cid = unknown_ids[0]
        elif unknown_ids and k % 55 == 11:
            cid = unknown_ids[1]
        else:
            cid = rng.choice(customer_ids)
        d = d_from + timedelta(days=rng.randint(0, days))
        is_return = rng.random() < 0.03
        fx = 1 / 25 if cust_country.get(cid, "CZ") == "SK" else 1
        ccy = "EUR" if fx != 1 else "CZK"
        for line, prod in enumerate(rng.sample(products, rng.randint(1, 3)), start=1):
            qty = rng.randint(10, 500)
            net = round(qty * prod[3] * fx * rng.uniform(0.85, 1.0), 2)
            if is_return:
                qty, net = -qty, -net
            rows.append(dict(order_id=oid, order_line=line, customer_id=cid, product_id=prod[0],
                             order_date=d.isoformat(), quantity=qty, net_value=net, currency=ccy,
                             order_type="RE" if is_return else "OR"))
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promo (Limigo): zákazníci a akce

# COMMAND ----------

def strip_legal(name):
    return name.replace(" s.r.o.", "").replace(" a.s.", "")


promo_customers = []
# a) spárovatelní podle reg_no (14 zákazníků ze SAP, jiná ID, lehce jiná jména a velikost písmen)
for k, idx in enumerate(range(0, 14)):
    c = customers[idx]
    promo_customers.append(dict(pid=f"C-{1001 + k}", name=strip_legal(c["name"]), reg_no=c["reg_no"],
                                city=c["city"].lower() if k % 3 == 0 else c["city"]))
# b) jen v promo, v SAP nejsou (první z nich se v delivery2 objeví v SAP)
promo_customers += [
    dict(pid="C-1201", name="Nový Zákazník Alfa s.r.o.", reg_no="45000001", city="Praha"),
    dict(pid="C-1202", name="Beta Retail s.r.o.", reg_no="45000002", city="Brno"),
    dict(pid="C-1203", name="Gama Market a.s.", reg_no="45000003", city="Košice"),
]
# c) PAST: stejné jméno jako SAP zákazník, ale jiné reg_no. Nesmí se spárovat podle jména.
promo_customers.append(dict(pid="C-1301", name=customers[0]["name"], reg_no="99999991", city="Praha"))
# d) bez reg_no, vyřeší se ručním mapováním ze SharePointu
promo_customers.append(dict(pid="C-1401", name=strip_legal(customers[16]["name"]), reg_no="", city=customers[16]["city"]))

PROMO_COLS = ["promo_id", "promo_customer_id", "customer_name", "reg_no", "city", "product_id",
              "start_date", "end_date", "discount_pct", "planned_volume", "status"]


def make_promo(n, pc=None):
    pc = pc or promo_customers[n % len(promo_customers)]
    start = date(2026, 10, 1) + timedelta(days=(n * 3) % 25)
    return dict(promo_id=f"PR-2026-{n + 1:04d}", promo_customer_id=pc["pid"], customer_name=pc["name"],
                reg_no=pc["reg_no"], city=pc["city"], product_id=products[(n * 5) % 12][0],
                start_date=start.isoformat(), end_date=(start + timedelta(days=14)).isoformat(),
                discount_pct=[10, 15, 20, 25, 30][n % 5], planned_volume=1000 + (n * 250) % 4000,
                status="planned")


promo_rows = [make_promo(n) for n in range(24)]

# Schválně špatné řádky (zůstávají ve všech snapshotech, protože je to full snapshot)
promo_rows[3]["promo_id"] = ""                                   # NEVALIDNÍ: chybí klíč
promo_rows[7]["discount_pct"] = 150                              # NEVALIDNÍ: sleva nad 100 %
promo_rows[11]["planned_volume"] = -500                          # NEVALIDNÍ: záporný objem
promo_rows[15]["end_date"] = (date.fromisoformat(promo_rows[15]["start_date"]) - timedelta(days=3)).isoformat()  # NEVALIDNÍ: konec před začátkem
promo_rows[19]["reg_no"] = f"  {promo_rows[19]['reg_no']} "      # ŠPINAVÉ (jde opravit): mezery kolem reg_no
promo_rows[21]["city"] = promo_rows[21]["city"].upper()           # ŠPINAVÉ (jde opravit): velká písmena
promo_rows[21]["customer_name"] = promo_rows[21]["customer_name"] + "   "  # ŠPINAVÉ: mezery na konci


def snapshot(day):
    """Full snapshot akcí k danému dni (1-4). Akce se mění, přibývají a jedna zmizí bez upozornění."""
    rows = [dict(r) for r in promo_rows]
    extra = []
    if day >= 2:
        for k in (0, 1, 2):
            rows[k]["status"] = "confirmed"
        extra.append(make_promo(24))
    if day >= 3:
        rows[4]["status"] = "confirmed"
        extra.append(make_promo(25))
    if day >= 4:
        rows[8]["status"] = "cancelled"
        extra.append(make_promo(26, pc=promo_customers[14]))     # C-1201 (později se objeví v SAP)
    rows += extra
    if day >= 2:
        rows = [r for i, r in enumerate(rows) if i != 5]         # PR-2026-0006 z full snapshotu zmizí
    return pd.DataFrame(rows, columns=PROMO_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nielsen: měsíční tržní data

# COMMAND ----------

CATEGORIES = ["Chipsy", "Křupky", "Ořechy", "Tyčinky"]
BRANDS = ["Nordic Snacks", "Competitor A", "Competitor B", "Competitor C"]
CAT_PRICE_CZ = {"Chipsy": 45, "Křupky": 35, "Ořechy": 95, "Tyčinky": 25}
PERIODS = ["2026-08", "2026-09", "2026-10"]
MARKET_COLS = ["period", "market", "category", "brand", "value_sales", "volume_sales"]


def build_market():
    """Vždy ve stejném pořadí, takže stejný seed dá stejná čísla i v delivery2."""
    rng = random.Random(seed + 1)
    out = {}
    for period in PERIODS:
        for market in ("CZ", "SK"):
            rows = []
            for cat in CATEGORIES:
                for b in BRANDS:
                    vol = rng.randint(20000, 120000) if market == "CZ" else rng.randint(8000, 60000)
                    price = CAT_PRICE_CZ[cat] / (1 if market == "CZ" else 25)
                    rows.append(dict(period=period, market=market, category=cat, brand=b,
                                     value_sales=round(vol * price * rng.uniform(0.9, 1.1), 2),
                                     volume_sales=vol))
            out[(market, period)] = rows
    return out


def write_market(market, period, rows, suffix=""):
    pdf = pd.DataFrame(rows, columns=MARKET_COLS)
    path = f"{LANDING}/market/{market}/market_{market}_{period}{suffix}.csv"
    if market == "SK":
        # Slovenský export: středník jako oddělovač a desetinná čárka (past pro Auto Loader a CAST)
        write_csv(pdf, path, sep=";", decimal=",")
    else:
        write_csv(pdf, path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scénář 1: `initial`

# COMMAND ----------

def run_initial():
    rng = random.Random(seed)

    # --- čistý začátek zdrojových souborů (checkpointy a schémata se nemažou) ---
    for sub in ("sharepoint", "promo", "market"):
        p = f"{LANDING}/{sub}"
        if os.path.exists(p):
            shutil.rmtree(p)

    # --- SAP (Delta tabulky v ofc_sap) ---
    print("SAP tabulky:")
    save_table(pd.DataFrame(customers), "customers", ts_cols=["changed_at"])
    save_table(pd.DataFrame(products, columns=["product_id", "name", "category", "unit_price"]),
               "products", dec_cols=["unit_price"])
    orders = gen_orders(rng, 600, date(2026, 8, 1), date(2026, 10, 31), 1,
                        [c["customer_id"] for c in customers], UNKNOWN_IDS)
    save_table(pd.DataFrame(orders), "sales_orders", date_cols=["order_date"], dec_cols=["net_value"])
    print(f"  customers={len(customers)}, products={len(products)}, sales_orders={len(orders)} řádků")

    # --- SharePoint ---
    print("SharePoint:")
    targets = []
    for i, c in enumerate(customers):
        for m, period in enumerate(PERIODS):
            targets.append(dict(customer_id=c["customer_id"], period=period,
                                target_value=(50 + (i * 7) % 40) * 1000 + m * 5000))
    targets[0]["target_value"] = ""                                         # NEVALIDNÍ: chybí cíl
    targets.append(dict(customer_id="0000188888", period="2026-10", target_value=75000))  # zákazník neexistuje
    write_csv(pd.DataFrame(targets), f"{LANDING}/sharepoint/sales_targets/sales_targets.csv")

    seg = pd.DataFrame([("HYP", "Hypermarket", "Moderní obchod"), ("SUP", "Supermarket", "Moderní obchod"),
                        ("DIS", "Diskont", "Moderní obchod"), ("CON", "Večerka", "Tradiční obchod")],
                       columns=["segment_code", "segment_name", "segment_group"])
    seg_dir = f"{LANDING}/sharepoint/customer_segments"
    os.makedirs(seg_dir, exist_ok=True)
    try:
        seg.to_excel(f"{seg_dir}/customer_segments.xlsx", index=False)
        print("  zapsáno: sharepoint/customer_segments/customer_segments.xlsx")
    except ImportError:
        write_csv(seg, f"{seg_dir}/customer_segments.csv")
        print("  POZOR: chybí openpyxl, segmenty jsou v CSV místo Excelu")

    mapping = pd.DataFrame([dict(promo_customer_id="C-1401", sap_customer_id=customers[16]["customer_id"],
                                 decided_by="data.steward", decided_on="2026-09-20",
                                 comment="Bez IČO, potvrzeno obchodním oddělením")])
    write_csv(mapping, f"{LANDING}/sharepoint/customer_manual_mapping/customer_manual_mapping.csv")

    # --- Promo: tři denní snapshoty ---
    print("Promo (Limigo):")
    for day in (1, 2, 3):
        write_csv(snapshot(day), f"{LANDING}/promo/promo_snapshots/promo_snapshot_2026-10-0{day}.csv")

    # --- Nielsen: srpen a září ---
    print("Market (Nielsen):")
    mk = build_market()
    for period in ("2026-08", "2026-09"):
        for market in ("CZ", "SK"):
            rows = [dict(r) for r in mk[(market, period)]]
            if (market, period) == ("CZ", "2026-09"):
                rows[3]["brand"] = ""                                       # NEVALIDNÍ: chybí značka
                rows[9]["value_sales"] = -abs(rows[9]["value_sales"])       # NEVALIDNÍ: záporný obrat
            if (market, period) == ("SK", "2026-08"):
                rows.append(dict(rows[0]))                                  # DUPLICITA: řádek dvakrát
            write_market(market, period, rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scénář 2: `delivery2` (změny v čase)

# COMMAND ----------

def run_delivery2():
    if not spark.catalog.tableExists(f"{catalog}.ofc_sap.customers"):
        raise RuntimeError("Nejdřív spusť scénář 'initial'.")
    rng = random.Random(seed + 100)

    # --- SAP: změna města, nový zákazník a master data pro dříve neznámé zákazníky ---
    print("SAP změny:")
    new_customers = [
        dict(customers[2], city="Ostrava-Poruba", changed_at="2026-11-02 09:30:00"),   # změna atributu -> SCD2
        dict(customer_id="0000100201", name="Nový Zákazník Alfa s.r.o.", reg_no="45000001", city="Praha",
             country="CZ", segment_code="HYP", changed_at="2026-11-02 10:00:00"),     # spáruje se s promo C-1201
        dict(customer_id=UNKNOWN_IDS[0], name="Opožděný Odběratel CZ s.r.o.", reg_no="46000001", city="Brno",
             country="CZ", segment_code="SUP", changed_at="2026-11-02 10:05:00"),     # doplní inferred member
        dict(customer_id=UNKNOWN_IDS[1], name="Opozdeny Odberatel SK s.r.o.", reg_no="46000002", city="Nitra",
             country="SK", segment_code="DIS", changed_at="2026-11-02 10:10:00"),     # doplní inferred member
    ]
    upsert(pd.DataFrame(new_customers), "customers", ["customer_id"], ts_cols=["changed_at"])
    cust_country["0000100201"] = "CZ"

    ids = [c["customer_id"] for c in customers] + ["0000100201"] + UNKNOWN_IDS
    orders = gen_orders(rng, 40, date(2026, 11, 1), date(2026, 11, 2), 100000, ids)
    upsert(pd.DataFrame(orders), "sales_orders", ["order_id", "order_line"],
           date_cols=["order_date"], dec_cols=["net_value"])
    print(f"  upsert: 4 zákazníků, {len(orders)} řádků objednávek (listopad)")

    # --- SharePoint: soubory se PŘEPISUJÍ pod stejným jménem (jako v reálu) ---
    print("SharePoint (přepsané soubory, Auto Loader je ve výchozím stavu znovu nenačte!):")
    all_ids = [c["customer_id"] for c in customers] + ["0000100201"]
    targets = [dict(customer_id=cid, period=p, target_value=(50 + (i * 7) % 40) * 1000 + m * 5000)
               for i, cid in enumerate(all_ids) for m, p in enumerate(PERIODS + ["2026-11"])]
    write_csv(pd.DataFrame(targets), f"{LANDING}/sharepoint/sales_targets/sales_targets.csv")

    mapping = pd.DataFrame([dict(promo_customer_id="C-1401", sap_customer_id=customers[17]["customer_id"],
                                 decided_by="data.steward", decided_on="2026-11-02",
                                 comment="Oprava: původní mapování bylo chybné")])
    write_csv(mapping, f"{LANDING}/sharepoint/customer_manual_mapping/customer_manual_mapping.csv")

    # --- Promo: nový snapshot + stejný soubor poslaný podruhé pod jiným názvem ---
    print("Promo (Limigo):")
    write_csv(snapshot(4), f"{LANDING}/promo/promo_snapshots/promo_snapshot_2026-10-04.csv")
    write_csv(snapshot(3), f"{LANDING}/promo/promo_snapshots/promo_snapshot_2026-10-03_copy.csv")

    # --- Nielsen: říjen + opravený září (stejný klíč, jiné hodnoty) ---
    print("Market (Nielsen):")
    mk = build_market()
    for market in ("CZ", "SK"):
        write_market(market, "2026-10", mk[(market, "2026-10")])
    fixed = [dict(r) for r in mk[("CZ", "2026-09")]]                        # bez schválně špatných řádků
    for k in (0, 5, 10):
        fixed[k]["value_sales"] = round(fixed[k]["value_sales"] * 1.05, 2)
    write_market("CZ", "2026-09", fixed, suffix="_corrected")

# COMMAND ----------

if scenario == "initial":
    run_initial()
else:
    run_delivery2()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Co v datech hledat (klíč k řešení)

# COMMAND ----------

ANSWER_KEY = """
OČEKÁVANÉ PROBLÉMY po scénáři 'initial'
  promo:      4 nevalidní řádky v každém snapshotu (chybí promo_id, sleva 150 %, záporný objem, konec před začátkem)
              3 špinavé, ale opravitelné (mezery v reg_no, VELKÁ písmena ve městě, mezery za jménem)
              PR-2026-0006 je v snapshotu 1 a od snapshotu 2 zmizí (full snapshot nehlásí smazání)
  customers:  14 promo zákazníků se páruje přes reg_no (jiná ID, jiná jména, různá velikost písmen)
              C-1201..C-1203 nejsou v SAP (nespárováno)
              C-1301 má stejné jméno jako SAP zákazník 0000100001, ale jiné reg_no (NESMÍ se spárovat)
              C-1401 nemá reg_no, spáruje se přes ruční mapování ze SharePointu
  sales:      zákazníci 0000199901 a 0000199902 jsou v objednávkách, ale ne v master datech (inferred member)
              ~3 % objednávek jsou vratky (order_type RE, záporné hodnoty), to je legitimní, ne chyba
  targets:    1 řádek bez cíle, 1 řádek se zákazníkem 0000188888, který neexistuje
  market:     CZ 2026-09: prázdná značka a záporný obrat; SK 2026-08: duplicitní řádek
              SK soubory mají ';' jako oddělovač a desetinnou čárku

OČEKÁVANÉ ZMĚNY po scénáři 'delivery2'
  zákazník 0000100003 změnil město (nová verze v SCD2)
  zákazník 0000100201 je nový a spáruje se s promo C-1201
  inferred members 0000199901 a 0000199902 se doplní skutečnými daty
  promo_snapshot_2026-10-03_copy.csv má stejný obsah jako snapshot z 3. 10. (duplicitní dodávka)
  sales_targets.csv a customer_manual_mapping.csv jsou PŘEPSANÉ pod stejným jménem
  market_CZ_2026-09_corrected.csv opravuje hodnoty za září (stejný klíč period+market+category+brand)
"""
print(ANSWER_KEY)