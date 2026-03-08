import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

def rand_date(start="2026-01-01", n=50):
    base = datetime.strptime(start, "%Y-%m-%d")
    return [(base + timedelta(days=random.randint(0, 60))).strftime("%Y%m%d") for _ in range(n)]

def rand_amount(n=50):
    return [round(random.uniform(500, 50000), 2) for _ in range(n)]

TAX_RATES = {"TX10": 0.10, "TX20": 0.20, "TX0": 0.0, "TX5": 0.05}

vendors = pd.DataFrame({
    "SAP_VENDOR_ID": [f"SAP_V{str(i).zfill(3)}" for i in range(1, 11)],
    "VENDOR_NAME":   ["Acme Corp","GlobalTech","NexusPay","FastLogistics","DataSystems","CloudBase","PayRoute","FinServ","TechBridge","CoreSupply"],
    "COUNTRY_CODE":  ["FR","DE","US","FR","GB","DE","US","FR","GB","DE"],
    "PAYMENT_TERMS": ["NET30","NET60","NET15","NET30","NET45","NET60","NET30","NET15","NET45","NET30"],
})
vendors.to_csv("data/vendor_ref.csv", index=False)

N = 60
vendor_ids = [random.choice(vendors["SAP_VENDOR_ID"].tolist()) for _ in range(N)]
gross_amounts = rand_amount(N)
tax_codes = [random.choice(["TX10","TX20","TX0","TX5"]) for _ in range(N)]
raw_statuses = (["PAID"]*25 + ["PENDING"]*15 + ["BLOCKED"]*10 + ["CANCELLED"]*10)
random.shuffle(raw_statuses)

sap_input = pd.DataFrame({
    "BUKRS": ["NXS"]*N,
    "BELNR": [f"SAP-2026-{str(i).zfill(5)}" for i in range(1, N+1)],
    "SAP_VENDOR_ID": vendor_ids,
    "WRBTR": gross_amounts,
    "MWSKZ": tax_codes,
    "BLDAT": rand_date(n=N),
    "ZFBDT": rand_date(n=N),
    "ZLSCH": ["T"]*N,
    "AUGDT": rand_date(n=N),
    "PAYMENT_STATUS_RAW": raw_statuses,
    "WAERS": ["EUR"]*N,
})
sap_input.to_csv("data/sap_input.csv", index=False)

filtered = sap_input[sap_input["PAYMENT_STATUS_RAW"].isin(["PAID","PENDING"])].copy()
filtered = filtered.merge(vendors, on="SAP_VENDOR_ID", how="left")
filtered["TAX_RATE"] = filtered["MWSKZ"].map(TAX_RATES)
filtered["NET_AMOUNT"] = (filtered["WRBTR"] * (1 - filtered["TAX_RATE"])).round(2)

talend_output = pd.DataFrame({
    "invoice_id":     filtered["BELNR"].values,
    "vendor_id":      filtered["SAP_VENDOR_ID"].values,
    "vendor_name":    filtered["VENDOR_NAME"].values,
    "country_code":   filtered["COUNTRY_CODE"].values,
    "payment_terms":  filtered["PAYMENT_TERMS"].values,
    "payment_status": filtered["PAYMENT_STATUS_RAW"].values,
    "currency":       filtered["WAERS"].values,
    "gross_amount":   filtered["WRBTR"].values,
    "tax_code":       filtered["MWSKZ"].values,
    "tax_rate":       filtered["TAX_RATE"].values,
    "net_amount":     filtered["NET_AMOUNT"].values,
    "posting_date":   filtered["BLDAT"].values,
    "payment_date":   filtered["ZFBDT"].values,
    "payment_method": filtered["ZLSCH"].values,
}).head(50).reset_index(drop=True)

talend_output.to_csv("data/talend_reference.csv", index=False)

def q(val):
    return "NULL" if val is None else f"'{str(val)}'"
def qn(val):
    return "NULL" if val is None else str(val)

sap_rows = []
for _, r in sap_input.iterrows():
    sap_rows.append(
        f"    ({q(r.BUKRS)}, {q(r.BELNR)}, {q(r.SAP_VENDOR_ID)}, "
        f"{qn(r.WRBTR)}, {q(r.MWSKZ)}, {q(r.BLDAT)}, "
        f"{q(r.ZFBDT)}, {q(r.ZLSCH)}, {q(r.AUGDT)}, "
        f"{q(r.PAYMENT_STATUS_RAW)}, {q(r.WAERS)})"
    )

vendor_rows = []
for _, r in vendors.iterrows():
    vendor_rows.append(
        f"    ({q(r.SAP_VENDOR_ID)}, {q(r.VENDOR_NAME)}, {q(r.COUNTRY_CODE)}, {q(r.PAYMENT_TERMS)})"
    )

# NO special characters - plain ASCII only for Windows compatibility
sql = "-- migration.sql\n"
sql += "-- SQL equivalent of JOB_NXS_T26_Payment_Status_SAP_Coupa\n"
sql += "-- Transformations: FILTER + JOIN x2 + RENAME + net_amount calculation\n\n"

sql += "-- Input: raw SAP data\n"
sql += "CREATE TABLE stg_sap_input (\n"
sql += "    bukrs               TEXT,\n"
sql += "    belnr               TEXT,\n"
sql += "    sap_vendor_id       TEXT,\n"
sql += "    wrbtr               REAL,\n"
sql += "    mwskz               TEXT,\n"
sql += "    bldat               TEXT,\n"
sql += "    zfbdt               TEXT,\n"
sql += "    zlsch               TEXT,\n"
sql += "    augdt               TEXT,\n"
sql += "    payment_status_raw  TEXT,\n"
sql += "    waers               TEXT\n"
sql += ");\n\n"
sql += "INSERT INTO stg_sap_input VALUES\n"
sql += ",\n".join(sap_rows) + ";\n\n"

sql += "-- Reference: vendor table\n"
sql += "CREATE TABLE stg_vendor_ref (\n"
sql += "    sap_vendor_id   TEXT,\n"
sql += "    vendor_name     TEXT,\n"
sql += "    country_code    TEXT,\n"
sql += "    payment_terms   TEXT\n"
sql += ");\n\n"
sql += "INSERT INTO stg_vendor_ref VALUES\n"
sql += ",\n".join(vendor_rows) + ";\n\n"

sql += "-- Reference: tax rates\n"
sql += "CREATE TABLE stg_tax_rates (\n"
sql += "    tax_code    TEXT,\n"
sql += "    tax_rate    REAL\n"
sql += ");\n\n"
sql += "INSERT INTO stg_tax_rates VALUES\n"
sql += "    ('TX10', 0.10),\n"
sql += "    ('TX20', 0.20),\n"
sql += "    ('TX0',  0.00),\n"
sql += "    ('TX5',  0.05);\n\n"

sql += "-- Output: stg_output (what Validation Agent compares against talend_reference.csv)\n"
sql += "CREATE TABLE stg_output AS\n"
sql += "SELECT\n"
sql += "    s.belnr                              AS invoice_id,\n"
sql += "    s.sap_vendor_id                      AS vendor_id,\n"
sql += "    v.vendor_name                        AS vendor_name,\n"
sql += "    v.country_code                       AS country_code,\n"
sql += "    v.payment_terms                      AS payment_terms,\n"
sql += "    s.payment_status_raw                 AS payment_status,\n"
sql += "    s.waers                              AS currency,\n"
sql += "    s.wrbtr                              AS gross_amount,\n"
sql += "    s.mwskz                              AS tax_code,\n"
sql += "    t.tax_rate                           AS tax_rate,\n"
sql += "    ROUND(s.wrbtr * (1 - t.tax_rate), 2) AS net_amount,\n"
sql += "    s.bldat                              AS posting_date,\n"
sql += "    s.zfbdt                              AS payment_date,\n"
sql += "    s.zlsch                              AS payment_method\n"
sql += "FROM stg_sap_input s\n"
sql += "LEFT JOIN stg_vendor_ref v ON s.sap_vendor_id = v.sap_vendor_id\n"
sql += "LEFT JOIN stg_tax_rates  t ON s.mwskz = t.tax_code\n"
sql += "WHERE s.payment_status_raw IN ('PAID', 'PENDING')\n"
sql += "LIMIT 50;\n"

# Write with explicit UTF-8 encoding
with open("scripts/migration.sql", "w", encoding="utf-8") as f:
    f.write(sql)

print("Done. Files generated:")
print(f"  sap_input.csv          : {len(sap_input)} rows")
print(f"  vendor_ref.csv         : {len(vendors)} rows")
print(f"  talend_reference.csv   : {len(talend_output)} rows")
print(f"  migration.sql          : {len(sql)} chars")
