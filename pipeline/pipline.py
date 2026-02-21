import sys
import pandas as pd

# 1) Ham veri / parametre alımı (dış dünya)
print("arguments:", sys.argv)

day = int(sys.argv[1])   # pipeline parametresi (hangi günü işliyoruz?)

# 2) Tablolaştırma (ham veriyi tabloya çevirme)
df = pd.DataFrame({
    "day": [1, 2],
    "number_passengers": [3, 4]
})

# 3) Dönüştürme (parametreyi veriyle birleştirme)
df["run_day"] = day      # pipeline hangi gün çalıştı bilgisi

# 4) Kontrol / preview (QA refleksi)
print(df.head())

# 5) Kaydetme (pipeline output'u)
output_file = f"output_day_{day}.parquet"
df.to_parquet(output_file)

# 6) Operasyonel log
print(f"Pipeline finished successfully for day = {day}")
