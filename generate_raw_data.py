import json
import random

OUTPUT = r"D:\mercari_github\html\data.js"

brands = ["NIKE","Levis","UNIQLO","GU","EDWIN"]

categories = [
    ("ファッション","メンズ","パンツ","デニム・ジーンズ","ストレートデニム"),
    ("ファッション","レディース","パンツ","デニム・ジーンズ","ストレートデニム")
]

data = []

for cat in categories:
    for b in brands:
        data.append({
            "ブランド": b,
            "L1": cat[0],
            "L2": cat[1],
            "L3": cat[2],
            "L4": cat[3],
            "L5": cat[4],
            "L6": "",
            "件数": random.randint(50,200),
            "需要スコア": random.randint(100,500),
            "平均単価": random.randint(2000,8000),
            "推定仕入価格": random.randint(500,2000),
            "推定利益/件": random.randint(500,3000),
            "利益率": random.randint(20,60),
            "平均売却秒数": random.randint(1000,100000),
            "即売れ率": random.randint(10,80),
            "年月": "2026-03"
        })

js = "const RAW = " + json.dumps(data, ensure_ascii=False, indent=2) + ";"

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(js)

print("RAW生成完了")