from pyspark.sql import functions as F
import math

JE = "workspace.silver.journal_entries"
THRESHOLD = 50000          # approval limit being tested

df = spark.table(JE)
debits = df.filter(F.col("SHKZG") == "S")      # one expense line per document, avoids double counting
total_docs = df.select("BELNR").distinct().count()
summary = []

def record(test, count, unit, note):
    summary.append((test, int(count), unit, note))
    print(f"{test}: {count:,} {unit}")

# ---- Test 1: Duplicate invoices (same vendor + amount + invoice date on different documents)
dup = (debits.groupBy("LIFNR", "DMBTR", "BLDAT")
       .agg(F.countDistinct("BELNR").alias("doc_count"))
       .filter("doc_count > 1"))
dup.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_duplicate_invoices")
record("1_duplicate_invoices", dup.count(), "groups", "Possible double payment")

# ---- Test 2: Round amounts (multiples of 1,000, at least 10,000)
rnd = debits.filter((F.col("DMBTR") >= 10000) & (F.col("DMBTR") % 1000 == 0))
rnd.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_round_amounts")
record("2_round_amounts", rnd.count(), "lines", "Possible estimates or fabricated entries")

# ---- Test 3: Weekend postings (counted per document, not per line)
wk = (debits.filter("IS_WEEKEND").select("BELNR", "BUDAT", "USNAM", "LIFNR", "DMBTR"))
wk.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_weekend_postings")
wk_n = wk.count()
record("3_weekend_postings", wk_n, "documents",
       f"{wk_n / total_docs:.1%} of all documents (synthetic dates are random; real data needs a holiday calendar)")

# ---- Test 4: Just below approval threshold, compared with the same-size band just above it
band = threshold_band = THRESHOLD * 0.05
below = debits.filter((F.col("DMBTR") >= THRESHOLD - band) & (F.col("DMBTR") < THRESHOLD))
above = debits.filter((F.col("DMBTR") > THRESHOLD) & (F.col("DMBTR") <= THRESHOLD + band))
below.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_below_threshold")
b_n, a_n = below.count(), above.count()
record("4_below_threshold", b_n, "lines",
       f"{b_n} just below vs {a_n} just above the {THRESHOLD:,} limit (ratio {b_n / max(a_n, 1):.2f}); a ratio well above 1 suggests splitting")

# ---- Test 5: Unbalanced documents (with a small tolerance)
bal = (df.groupBy("BELNR").agg(
        F.sum(F.when(F.col("SHKZG") == "S", F.col("DMBTR")).otherwise(0)).alias("total_debit"),
        F.sum(F.when(F.col("SHKZG") == "H", F.col("DMBTR")).otherwise(0)).alias("total_credit"))
       .withColumn("difference", F.round(F.col("total_debit") - F.col("total_credit"), 2)))
unb = bal.filter(F.abs("difference") > 0.01)
unb.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_unbalanced_documents")
record("5_unbalanced_documents", unb.count(), "documents", "Debits do not equal credits")

# ---- Test 6: Benford's Law on leading digits of debit amounts
pos = debits.filter(F.col("DMBTR") >= 1)
digits = (pos.withColumn("first_digit",
              F.floor(F.col("DMBTR") / F.pow(F.lit(10.0), F.floor(F.log10("DMBTR")))).cast("int"))
          .groupBy("first_digit").agg(F.count("*").alias("actual_count"))
          .orderBy("first_digit").collect())
counts = {r["first_digit"]: r["actual_count"] for r in digits}
n = sum(counts.get(d, 0) for d in range(1, 10))
rows, chi, mad = [], 0.0, 0.0
for d in range(1, 10):
    exp_p = math.log10(1 + 1 / d)
    act_p = counts.get(d, 0) / n
    chi += (counts.get(d, 0) - exp_p * n) ** 2 / (exp_p * n)
    mad += abs(act_p - exp_p) / 9
    rows.append((d, counts.get(d, 0), round(act_p * 100, 2), round(exp_p * 100, 2), round((act_p - exp_p) * 100, 2)))
benford = spark.createDataFrame(rows, "digit int, actual_count long, actual_pct double, expected_pct double, deviation_pct double")
benford.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_benford")
verdict = ("close conformity" if mad < 0.006 else "acceptable conformity" if mad < 0.012
           else "marginal conformity" if mad < 0.015 else "non-conformity")
record("6_benford_law", 1 if mad >= 0.015 else 0, "flag (1 = MAD shows non-conformity)",
       f"MAD {mad:.4f} = {verdict}; chi-square {chi:.1f} (5% critical value 15.5)")

# ---- Save summary for the dashboard
spark.createDataFrame(summary, "test string, exceptions long, unit string, note string") \
     .write.format("delta").mode("overwrite").saveAsTable("workspace.gold.audit_summary")
display(spark.table("workspace.gold.audit_summary"))
display(benford)
