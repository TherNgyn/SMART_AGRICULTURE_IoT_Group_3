import os, pandas as pd
from dotenv import load_dotenv
load_dotenv()

raw = os.environ.get("RAW_CSV", "./data/data.csv")
train = os.environ.get("TRAIN_CSV", "./data/train_45000.csv")
stream = os.environ.get("STREAM_CSV", "./data/stream_15000.csv")

df = pd.read_csv(raw)
df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)  # shuffle

assert len(df) >= 60000, "Dataset cần >= 60,000 dòng."

df.iloc[:45000].to_csv(train, index=False)
df.iloc[45000:45000+15000].to_csv(stream, index=False)

print(f" Wrote {train} (45k) and {stream} (15k)")
