import os,requests,pathlib
from dotenv import load_dotenv
load_dotenv()
URL=os.getenv("SUPABASE_URL")
KEY=os.getenv("SUPABASE_KEY")
H={"apikey":KEY,"Authorization":"Bearer "+KEY,"x-upsert":"true"}
OUT=pathlib.Path("output")
files=list(OUT.glob("*.json"))+[OUT/"mercari_dashboard.html"]
for f in files:
    ct="application/json" if f.suffix==".json" else "text/html"
    r=requests.post(URL+"/storage/v1/object/mercari-data/"+f.name,headers=dict(**H,**{"Content-Type":ct}),data=f.read_bytes())
    print(f.name,r.status_code)
print("Done")
