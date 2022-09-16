import pandas as pd
import numpy as np

# Tariffs
tariffs = pd.read_csv('data/01_raw/Ht_nt_times.csv', sep=',')
columns = ["hthrswd", "hthrssat", "hthrsso"]
for column in columns:
    tariffs[column] = tariffs[column].str.split(";")
tariffs = tariffs.melt(
    id_vars="timetariffId",
    value_name="Hour",
    var_name="WeekdayType"
)
tariffs = tariffs.dropna(subset=["Hour"])
tariffs["WeekdayType"] = tariffs["WeekdayType"].map(
    {
        "hthrswd": "Weekday",
        "hthrssat": "Saturday",
        "hthrsso": "Sunday"
    }
)
tariffs = tariffs.explode("Hour")
tariffs["Hour"] = pd.to_numeric(tariffs["Hour"].str.split(":00", expand=True)[0], downcast="integer")
tariffs["IsHighTariff"] = True

# Chargers data
df_mfh = pd.read_csv('data/01_raw/charging_data_mfh.csv')
df_mfh["chargerType"] = "MFH"
df_work = pd.read_csv('data/01_raw/charging_data_work.csv')
df_work["chargerType"] = "Work"
df_poi = pd.read_csv('data/01_raw/charging_data_poi.csv')
df_poi["chargerType"] = "POI"
df_chargers = pd.concat([df_mfh, df_work, df_poi], ignore_index=True)
df_chargers["timestamp"] = pd.to_datetime(df_chargers["timestamp"], utc=True)
df_chargers["timestamp"] = df_chargers["timestamp"].dt.tz_convert("Europe/Zurich")
df_chargers["chargeLogId"] = df_chargers.groupby(["charge_log_id", "chargerType"]).ngroup()
df_chargers = df_chargers.sort_values(["chargeLogId", "timestamp"])
df_chargers["Hour"] = df_chargers["timestamp"].dt.hour
df_chargers["Weekday"] = df_chargers["timestamp"].dt.day_name()
df_chargers["Month"] = df_chargers["timestamp"].dt.month
df_chargers["WeekdayType"] = np.where(
    df_chargers["Weekday"].isin(["Saturday", "Sunday"]),
    df_chargers["Weekday"],
    "Weekday"
)
df_chargers = df_chargers.merge(tariffs, how="left")
df_chargers["IsHighTariff"] = df_chargers["IsHighTariff"].fillna(False)

df_chargers["plugInTime"] = df_chargers.groupby("chargeLogId")["timestamp"].transform(
    "min"
)
df_chargers["plugOutTime"] = df_chargers.groupby("chargeLogId")["timestamp"].transform(
    "max"
)

# Non zero data
non_zero_chargers = df_chargers.query("increment > 0").reset_index(drop=True)
non_zero_chargers["chargeStartTime"] = non_zero_chargers.groupby("chargeLogId")[
    "timestamp"
].transform("min")
non_zero_chargers["chargeEndTime"] = non_zero_chargers.groupby("chargeLogId")[
    "timestamp"
].transform("max")
non_zero_chargers = non_zero_chargers[
    ["chargeLogId", "chargeStartTime", "chargeEndTime"]
]
non_zero_chargers = non_zero_chargers.drop_duplicates(subset=["chargeLogId"])
df_chargers = df_chargers.merge(non_zero_chargers, how="left")

# to csv
print("to csv")
df_chargers.to_csv("data/02_cleaned/cleaned_data.csv", index=False)
