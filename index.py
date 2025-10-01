import pandas as pd

df = pd.read_csv("result.csv")

# Replace 'f' or 't' strings in integer column with NaN
df["claimed_by_user_id"] = pd.to_numeric(df["claimed_by_user_id"], errors='coerce')  # turn invalid to NaN

# Ensure 'is_disputed' is boolean
df["is_disputed"] = df["is_disputed"].map({"t": True, "f": False, "true": True, "false": False})

df.to_csv("result_clean.csv", index=False)
