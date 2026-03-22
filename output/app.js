var CUR_FROM="",CUR_TO="";
var CUR_L1="",CUR_L2="",CUR_L3="",CUR_L4="",CUR_L5="",CUR_L6="",CUR_BRAND="";
var AC_IDX=-1;

function time_fmt(sec){
  if(sec===null||sec===undefined)return"-";
  var s=parseFloat(sec);if(isNaN(s))return"-";
  if(s<86400){var h=s/3600;return h<1?Math.round(s/60)+"分":h.toFixed(1)+"時間";}
  return(s/86400).toFixed(1)+"日";
}
function onBrandInput(val){
  AC_IDX=-1;var list=document.getElementById("ac-list");
  if(!val){list.classList.remove("show");list.innerHTML="";return;}
  var q=val.toLowerCase();
  var matches=ALL_BRANDS.filter(function(b){return b.toLowerCase().indexOf(q)>=0;});
  if(!matches.length){list.classList.remove("show");list.innerHTML="";return;}
  var html="";
  matches.forEach(function(b){
    var esc=b.replace(/&/g,"&amp;").replace(/'/g,"&#39;").replace(/"/g,"&quot;");
    var hi=b.replace(new RegExp(val.replace(/[.*+?^${}()|[\]\\]/g,"\\$&"),"gi"),
      function(m){return"<b style='color:#1971c2'>"+m+"</b>";});
    html+="<div class='ac-item' data-val='"+esc+"' onmousedown='selectBrand(this.dataset.val)'>"+hi+"</div>";
  });
  html+="<div class='ac-count'>"+matches.length+"件一致</div>";
  list.innerHTML=html;list.classList.add("show");
}
function onBrandKey(e){
  var list=document.getElementById("ac-list");var items=list.querySelectorAll(".ac-item");
  if(e.key==="ArrowDown"){e.preventDefault();AC_IDX=Math.min(AC_IDX+1,items.length-1);items.forEach(function(el,i){el.classList.toggle("active",i===AC_IDX);});}
  else if(e.key==="ArrowUp"){e.preventDefault();AC_IDX=Math.max(AC_IDX-1,0);items.forEach(function(el,i){el.classList.toggle("active",i===AC_IDX);});}
  else if(e.key==="Enter"){if(AC_IDX>=0&&items[AC_IDX]){selectBrand(items[AC_IDX].getAttribute("data-val"));}else{applyFilter();}}
  else if(e.key==="Escape"){list.classList.remove("show");}
}
function selectBrand(val){
  document.getElementById("brand-input").value=val;
  document.getElementById("ac-list").classList.remove("show");
  CUR_BRAND=val;applyFilter();
}
document.addEventListener("click",function(e){
  if(!e.target.closest(".ac-wrap"))document.getElementById("ac-list").classList.remove("show");
});
function setSel(id,opts,disabled){
  var labels={"sl1":"全L1","sl2":"全L2","sl3":"全L3","sl4":"全L4","sl5":"全L5","sl6":"全L6"};
  var s=document.getElementById(id);
  s.innerHTML="<option value=''>"+labels[id]+"</option>";
  opts.forEach(function(v){var o=document.createElement("option");o.value=v;o.textContent=v;s.appendChild(o);});
  s.disabled=disabled;
}
function initSel(){
  var l1keys=Object.keys(CAT).sort();setSel("sl1",l1keys,false);
  if(DEFAULT_L1&&l1keys.length===1){
    document.getElementById("sl1").value=DEFAULT_L1;CUR_L1=DEFAULT_L1;
    setSel("sl2",CAT[DEFAULT_L1]?Object.keys(CAT[DEFAULT_L1]).sort():[],false);
  }
  document.getElementById("sl1").addEventListener("change",function(){
    var l1=this.value;setSel("sl2",l1&&CAT[l1]?Object.keys(CAT[l1]).sort():[],!l1);
    setSel("sl3",[],true);setSel("sl4",[],true);setSel("sl5",[],true);setSel("sl6",[],true);
  });
  document.getElementById("sl2").addEventListener("change",function(){
    var l1=document.getElementById("sl1").value,l2=this.value;
    setSel("sl3",l1&&l2&&CAT[l1]&&CAT[l1][l2]?Object.keys(CAT[l1][l2]).sort():[],!l2);
    setSel("sl4",[],true);setSel("sl5",[],true);setSel("sl6",[],true);
  });
  document.getElementById("sl3").addEventListener("change",function(){
    var l1=document.getElementById("sl1").value,l2=document.getElementById("sl2").value,l3=this.value;
    setSel("sl4",l1&&l2&&l3&&CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3]?Object.keys(CAT[l1][l2][l3]).sort():[],!l3);
    setSel("sl5",[],true);setSel("sl6",[],true);
  });
  document.getElementById("sl4").addEventListener("change",function(){
    var l1=document.getElementById("sl1").value,l2=document.getElementById("sl2").value;
    var l3=document.getElementById("sl3").value,l4=this.value;
    setSel("sl5",l1&&l2&&l3&&l4&&CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3]&&CAT[l1][l2][l3][l4]?Object.keys(CAT[l1][l2][l3][l4]).sort():[],!l4);
    setSel("sl6",[],true);
  });
  document.getElementById("sl5").addEventListener("change",function(){
    var l1=document.getElementById("sl1").value,l2=document.getElementById("sl2").value;
    var l3=document.getElementById("sl3").value,l4=document.getElementById("sl4").value,l5=this.value;
    var opts=l1&&l2&&l3&&l4&&l5&&CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3]&&CAT[l1][l2][l3][l4]&&CAT[l1][l2][l3][l4][l5]?Object.keys(CAT[l1][l2][l3][l4][l5]).sort():[];
    setSel("sl6",opts,!l5||opts.length===0);
  });
}
function applyFilter(){
  CUR_FROM=document.getElementById("dfrom").value;CUR_TO=document.getElementById("dto").value;
  CUR_L1=document.getElementById("sl1").value;CUR_L2=document.getElementById("sl2").value;
  CUR_L3=document.getElementById("sl3").value;CUR_L4=document.getElementById("sl4").value;
  CUR_L5=document.getElementById("sl5").value;CUR_L6=document.getElementById("sl6").value;
  CUR_BRAND=document.getElementById("brand-input").value.trim();refreshUI();
}
function resetFilter(){
  var today=new Date();
  var ymd=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0")+"-"+String(today.getDate()).padStart(2,"0");
  document.getElementById("dfrom").value=DB_START;document.getElementById("dto").value=ymd;
  document.getElementById("brand-input").value="";document.getElementById("ac-list").classList.remove("show");
  ["sl2","sl3","sl4","sl5","sl6"].forEach(function(id){document.getElementById(id).value="";});
  if(DEFAULT_L1){
    document.getElementById("sl1").value=DEFAULT_L1;CUR_L1=DEFAULT_L1;
    setSel("sl2",CAT[DEFAULT_L1]?Object.keys(CAT[DEFAULT_L1]).sort():[],false);
  }else{
    document.getElementById("sl1").value="";
    ["sl2","sl3","sl4","sl5","sl6"].forEach(function(id){document.getElementById(id).disabled=true;});
    CUR_L1="";
  }
  CUR_FROM=DB_START;CUR_TO=ymd;CUR_L2="";CUR_L3="";CUR_L4="";CUR_L5="";CUR_L6="";CUR_BRAND="";
  refreshUI();
}
function getFiltered(){
  var fromYM=CUR_FROM.slice(0,7),toYM=CUR_TO.slice(0,7);var q=CUR_BRAND.toLowerCase();
  return RAW.filter(function(r){
    if(r["年月"]&&r["年月"]!=="不明"){if(r["年月"]<fromYM||r["年月"]>toYM)return false;}
    if(CUR_L1&&r.L1!==CUR_L1)return false;if(CUR_L2&&r.L2!==CUR_L2)return false;
    if(CUR_L3&&r.L3!==CUR_L3)return false;if(CUR_L4&&r.L4!==CUR_L4)return false;
    if(CUR_L5&&r.L5!==CUR_L5)return false;if(CUR_L6&&r.L6!==CUR_L6)return false;
    if(q&&r["ブランド"].toLowerCase().indexOf(q)<0)return false;return true;
  });
}
function fparts(){
  var p=[];
  if(CUR_L1)p.push(CUR_L1);if(CUR_L2)p.push(CUR_L2);if(CUR_L3)p.push(CUR_L3);
  if(CUR_L4)p.push(CUR_L4);if(CUR_L5)p.push(CUR_L5);if(CUR_L6)p.push(CUR_L6);
  if(CUR_BRAND)p.push("🏷️"+CUR_BRAND);return p;
}
function refreshUI(){
  var f=getFiltered(),sb=document.getElementById("sbar"),parts=fparts();
  var fromYM=CUR_FROM.slice(0,7),toYM=CUR_TO.slice(0,7);
  var dbFromYM=DB_START.slice(0,7);
  var today=new Date();
  var todayYM=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0");
  var hasFilter=parts.length>0||fromYM!==dbFromYM||toYM!==todayYM;
  if(hasFilter){
    sb.className="sbar filtered";var info="📅 "+fromYM+" 〜 "+toYM;
    if(parts.length)info+=" 🔍 "+parts.join(" > ");
    sb.innerHTML=info+" — <b>"+f.length.toLocaleString()+"</b> 件";
  }else{sb.className="sbar";sb.innerHTML="全 <b>"+f.length.toLocaleString()+"</b> 件表示中";}
}
function ssum(a,k){return a.reduce(function(s,r){return s+(parseFloat(r[k])||0);},0);}
function aavg(a,k){return a.length?ssum(a,k)/a.length:0;}
function yen(n){if(n===null||n===undefined)return"-";return"¥"+Math.round(n).toLocaleString("ja-JP");}
function pct(n){if(n===null||n===undefined)return"-";return parseFloat(n).toFixed(1)+"%";}
function srt(a,k,asc){return a.slice().sort(function(x,y){var av=parseFloat(x[k])||0,bv=parseFloat(y[k])||0;return asc?av-bv:bv-av;});}
var BC=[{k:"__r",lb:"Rank"},{k:"ブランド",lb:"ブランド"},{k:"L1",lb:"L1"},{k:"L2",lb:"L2"},{k:"L3",lb:"L3"},{k:"L4",lb:"L4"},{k:"L5",lb:"L5"},{k:"L6",lb:"L6"}];
var CC=[{k:"__r",lb:"Rank"},{k:"L1",lb:"L1"},{k:"L2",lb:"L2"},{k:"L3",lb:"L3"},{k:"L4",lb:"L4"},{k:"L5",lb:"L5"},{k:"L6",lb:"L6"}];
function grpBrand(rows){
  var m={};
  rows.forEach(function(r){var k=r["ブランド"]+"|"+r.L1+"|"+r.L2+"|"+r.L3+"|"+r.L4+"|"+r.L5+"|"+r.L6;if(!m[k])m[k]=[];m[k].push(r);});
  return Object.keys(m).map(function(k){
    var g=m[k],t=g[0];
    return{ブランド:t["ブランド"],L1:t.L1,L2:t.L2,L3:t.L3,L4:t.L4,L5:t.L5,L6:t.L6,
      件数:ssum(g,"件数"),需要スコア:ssum(g,"需要スコア"),平均単価:Math.round(aavg(g,"平均単価")),
      推定仕入価格:Math.round(aavg(g,"推定仕入価格")),推定利益:Math.round(aavg(g,"推定利益/件")),
      利益率:parseFloat(aavg(g,"利益率").toFixed(1)),平均売却秒数:Math.round(aavg(g,"平均売却秒数")),
      即売れ率:parseFloat(aavg(g,"即売れ率").toFixed(1)),送料:t["送料"],
      損益分岐点:Math.round(aavg(g,"損益分岐点")),仕入れスコア:ssum(g,"仕入れスコア")};
  });
}
function grpCat(rows){
  var m={};
  rows.forEach(function(r){var k=r.L1+"|"+r.L2+"|"+r.L3+"|"+r.L4+"|"+r.L5+"|"+r.L6;if(!m[k])m[k]=[];m[k].push(r);});
  return Object.keys(m).map(function(k){
    var g=m[k],t=g[0];
    return{L1:t.L1,L2:t.L2,L3:t.L3,L4:t.L4,L5:t.L5,L6:t.L6,
      件数:ssum(g,"件数"),需要スコア:ssum(g,"需要スコア"),平均単価:Math.round(aavg(g,"平均単価")),
      推定仕入価格:Math.round(aavg(g,"推定仕入価格")),推定利益:Math.round(aavg(g,"推定利益/件")),
      利益率:parseFloat(aavg(g,"利益率").toFixed(1)),平均売却秒数:Math.round(aavg(g,"平均売却秒数")),
      即売れ率:parseFloat(aavg(g,"即売れ率").toFixed(1)),送料:t["送料"]};
  });
}
function tbl(rows,cols){
  if(!rows||!rows.length)return"<div class='nodata'>該当データなし</div>";
  var h="<div class='tw'><table><thead><tr>";
  cols.forEach(function(c){h+="<th>"+c.lb+"</th>";});h+="</tr></thead><tbody>";
  rows.forEach(function(r,i){
    h+="<tr>";
    cols.forEach(function(c){
      var v;if(c.k==="__r")v="<span class='rk'>"+(i+1)+"</span>";
      else if(c.f)v=c.f(r[c.k]);
      else v=(r[c.k]!==undefined&&r[c.k]!==null&&r[c.k]!=="")?r[c.k]:"-";
      h+="<td"+(c.cl?" class='"+c.cl+"'":"")+">"+v+"</td>";
    });h+="</tr>";
  });return h+"</tbody></table></div>";
}
function krow(items){return"<div class='krow'>"+items.map(function(x){return"<div class='kpi'><div class='kv'>"+x[0]+"</div><div class='kl'>"+x[1]+"</div></div>";}).join("")+"</div>";}
function badge(){
  var p=fparts();var fromYM=CUR_FROM.slice(0,7),toYM=CUR_TO.slice(0,7);
  var dbFromYM=DB_START.slice(0,7);
  var today=new Date();var todayYM=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0");
  var out="";
  if(fromYM!==dbFromYM||toYM!==todayYM)out+="<span style='font-size:10px;background:#e7f0ff;color:#1971c2;border:1px solid #c5d5f5;border-radius:4px;padding:2px 7px;margin-right:4px;'>📅 "+fromYM+" 〜 "+toYM+"</span>";
  if(p.length)out+="<span style='font-size:10px;background:#dbeafe;color:#1558a0;border:1px solid #c5d5f5;border-radius:4px;padding:2px 7px;'>🔍 "+p.join(" > ")+"</span>";
  return out?"<div style='margin-bottom:10px;'>"+out+"</div>":"";
}
var N={
  件数:{k:"件数",lb:"件数",cl:"nr"},需要:{k:"需要スコア",lb:"需要スコア",cl:"nr"},
  単価:{k:"平均単価",lb:"平均単価",f:yen,cl:"nr"},仕入:{k:"推定仕入価格",lb:"仕入価格",f:yen,cl:"nr"},
  利益:{k:"推定利益",lb:"推定利益/件",f:yen,cl:"nr"},利益率:{k:"利益率",lb:"利益率",f:pct,cl:"nr"},
  売却日:{k:"平均売却秒数",lb:"売却時間",f:time_fmt,cl:"nr"},即売:{k:"即売れ率",lb:"即売れ率",f:pct,cl:"nr"},
  送料:{k:"送料",lb:"送料",f:yen,cl:"nr"},スコア:{k:"仕入れスコア",lb:"仕入スコア",cl:"nr"}
};
function genPop(n){
  var raw=getFiltered();
  if(!raw.length)return"<div class='nodata'>絞込条件に一致するデータがありません<br><small>期間・カテゴリ・ブランド条件を確認してください</small></div>";
  var bd=grpBrand(raw),cd=grpCat(raw),bk=badge();
  if(n===1){var d=srt(bd,"需要スコア",false),t=d[0]||{};return bk+krow([[ssum(raw,"件数").toLocaleString()+"件","絞込総件数"],["1位:"+(t["ブランド"]||"-"),(t["件数"]||0).toLocaleString()+"件"],[yen(t["平均単価"]),"1位平均単価"],[pct(t["利益率"]),"1位利益率"]])+"<div class='ins'><h4>💡 ポイント</h4><ul><li>需要スコア降順 / ブランド別集計</li><li>売却時間はDBから実計算（0日の場合は時間表示）</li></ul></div><div class='sec'>ブランド売上ランキング 全件</div><button class='bcsv' onclick='dlCSV(1)'>⬇ CSV</button>"+tbl(d,BC.concat([N.件数,N.需要,N.単価,N.仕入,N.利益,N.利益率,N.売却日,N.即売,N.送料]));}
  if(n===2){var d=srt(bd,"平均売却秒数",true),t=d[0]||{};return bk+krow([[time_fmt(t["平均売却秒数"]),"最速1位:"+(t["ブランド"]||"-")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均売却時間"],[pct(parseFloat(aavg(d,"即売れ率").toFixed(1))),"平均即売れ率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>ブランド回転率 全件</div><button class='bcsv' onclick='dlCSV(2)'>⬇ CSV</button>"+tbl(d,BC.concat([N.売却日,N.即売,N.件数,N.単価,N.利益]));}
  if(n===3){var d=srt(bd,"平均単価",false),t=d[0]||{};return bk+krow([[yen(t["平均単価"]),"1位:"+(t["ブランド"]||"-")],[yen(Math.round(aavg(d,"平均単価"))),"全体平均"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>ブランド平均売却価格 全件</div><button class='bcsv' onclick='dlCSV(3)'>⬇ CSV</button>"+tbl(d,BC.concat([N.単価,N.仕入,N.利益,N.利益率,N.件数]));}
  if(n===4){var d=srt(bd,"平均売却秒数",true),t=d[0]||{};return bk+krow([[time_fmt(t["平均売却秒数"]),"最速:"+(t["ブランド"]||"-")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均"],[pct(parseFloat(aavg(d,"即売れ率").toFixed(1))),"即売れ率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>ブランド平均売却時間 全件</div><button class='bcsv' onclick='dlCSV(4)'>⬇ CSV</button>"+tbl(d,BC.concat([N.売却日,N.即売,N.件数,N.単価]));}
  if(n===5){var d=srt(bd,"推定利益",false),t=d[0]||{};return bk+krow([[yen(t["推定利益"]),"1位:"+(t["ブランド"]||"-")],[pct(t["利益率"]),"1位利益率"],[yen(Math.round(aavg(d,"推定利益"))),"平均推定利益/件"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>ブランド利益率 全件</div><button class='bcsv' onclick='dlCSV(5)'>⬇ CSV</button>"+tbl(d,BC.concat([N.利益,N.利益率,N.単価,N.件数]));}
  if(n===6){var d=srt(cd,"需要スコア",false),t=d[0]||{},tc=[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">");return bk+krow([[(t["件数"]||0).toLocaleString()+"件","1位:"+tc],[yen(Math.round(aavg(d,"平均単価"))),"平均単価"],[pct(parseFloat(aavg(d,"利益率").toFixed(1))),"平均利益率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>カテゴリ売上ランキング 全件</div><button class='bcsv' onclick='dlCSV(6)'>⬇ CSV</button>"+tbl(d,CC.concat([N.件数,N.需要,N.単価,N.利益,N.利益率,N.売却日,N.即売]));}
  if(n===7){var d=srt(cd,"平均売却秒数",true),t=d[0]||{};return bk+krow([[time_fmt(t["平均売却秒数"]),"最速:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均"],[pct(parseFloat(aavg(d,"即売れ率").toFixed(1))),"即売れ率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>カテゴリ回転率 全件</div><button class='bcsv' onclick='dlCSV(7)'>⬇ CSV</button>"+tbl(d,CC.concat([N.売却日,N.即売,N.件数,N.単価]));}
  if(n===8){var d=srt(cd,"平均単価",false),t=d[0]||{};return bk+krow([[yen(t["平均単価"]),"1位:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[yen(Math.round(aavg(d,"平均単価"))),"全体平均"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>カテゴリ平均価格 全件</div><button class='bcsv' onclick='dlCSV(8)'>⬇ CSV</button>"+tbl(d,CC.concat([N.単価,N.仕入,N.利益,N.利益率,N.件数]));}
  if(n===9){var d=srt(cd,"平均売却秒数",true),t=d[0]||{};return bk+krow([[time_fmt(t["平均売却秒数"]),"最速:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>カテゴリ平均売却時間 全件</div><button class='bcsv' onclick='dlCSV(9)'>⬇ CSV</button>"+tbl(d,CC.concat([N.売却日,N.即売,N.件数,N.単価]));}
  if(n===10){var d=srt(cd,"推定利益",false),t=d[0]||{};return bk+krow([[yen(t["推定利益"]),"1位:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[pct(t["利益率"]),"1位利益率"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>カテゴリ利益率 全件</div><button class='bcsv' onclick='dlCSV(10)'>⬇ CSV</button>"+tbl(d,CC.concat([N.利益,N.利益率,N.単価,N.件数]));}
  if(n>=11&&n<=15){var sk={11:"需要スコア",12:"平均売却秒数",13:"平均単価",14:"平均売却秒数",15:"推定利益"};var sa={11:false,12:true,13:false,14:true,15:false};var st={11:"売れる商品ランキング",12:"商品回転率",13:"商品平均売却価格",14:"商品平均売却時間",15:"商品利益率"};var d=srt(bd,sk[n],sa[n]),t=d[0]||{};var kv=(n===12||n===14)?time_fmt(t["平均売却秒数"]):(n===13||n===15)?yen(n===13?t["平均単価"]:t["推定利益"]):(t["件数"]||0).toLocaleString()+"件";return bk+krow([[kv,"1位:"+(t["ブランド"]||"-")],[d.length.toLocaleString()+"件","集計件数"],["-",""],["-",""]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,BC.concat([N.件数,N.需要,N.単価,N.利益,N.利益率,N.売却日,N.即売]));}
  if(n>=16&&n<=20){var bands=[["〜3,000円",0,3e3],["3,000〜5,000円",3e3,5e3],["5,000〜10,000円",5e3,1e4],["10,000〜30,000円",1e4,3e4],["30,000円〜",3e4,1e9]];var sk={16:"件数",17:"平均売却秒数",18:"利益率",19:"件数",20:"件数"};var sa={16:false,17:true,18:false,19:false,20:false};var st={16:"売れる価格帯",17:"価格帯別回転率",18:"価格帯別利益率",19:"平均値下げ幅",20:"値下げ売却率"};var d=srt(bands.map(function(b){var g=raw.filter(function(r){var p=parseFloat(r["平均単価"])||0;return p>=b[1]&&p<b[2];});if(!g.length)return null;return{価格帯:b[0],件数:ssum(g,"件数"),平均単価:Math.round(aavg(g,"平均単価")),推定仕入価格:Math.round(aavg(g,"推定仕入価格")),推定利益:Math.round(aavg(g,"推定利益/件")),利益率:parseFloat(aavg(g,"利益率").toFixed(1)),平均売却秒数:Math.round(aavg(g,"平均売却秒数")),即売れ率:parseFloat(aavg(g,"即売れ率").toFixed(1)),損益分岐点:Math.round(aavg(g,"損益分岐点"))};}).filter(Boolean),sk[n],sa[n]);var t=d[0]||{};return krow([[t["価格帯"]||"-","1位価格帯"],[(t["件数"]||0).toLocaleString()+"件","件数"],[pct(t["利益率"]),"利益率"],[pct(t["即売れ率"]),"即売れ率"]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,[{k:"__r",lb:"Rank"},{k:"価格帯",lb:"価格帯"},N.件数,N.単価,N.仕入,N.利益,N.利益率,N.売却日,N.即売,{k:"損益分岐点",lb:"損益分岐点",f:yen,cl:"nr"}]);}
  if(n>=21&&n<=25){var m={};raw.forEach(function(r){[r["ブランド"],r.L3,r.L4,r.L5].filter(Boolean).forEach(function(kw){m[kw]=(m[kw]||0)+(parseInt(r["件数"])||1);});});var d=srt(Object.keys(m).map(function(k){return{キーワード:k,出現頻度:m[k]};}),"出現頻度",false);var t=d[0]||{};var st={21:"タイトルKWランキング",22:"検索ヒットKW",23:"ブランド+商品名",24:"型番効果",25:"送料表記効果"};return bk+krow([[t["キーワード"]||"-","1位"],[(t["出現頻度"]||0).toLocaleString(),"出現頻度"],[d.length.toLocaleString()+"件","総KW数"],["-",""]])+"<div class='sec'>"+st[n]+" 全件</div>"+tbl(d.slice(0,100),[{k:"__r",lb:"Rank"},{k:"キーワード",lb:"キーワード"},{k:"出現頻度",lb:"出現頻度",cl:"nr"}]);}
  if(n>=26&&n<=30){var st={26:"急上昇ブランド",27:"急上昇カテゴリ",28:"価格上昇商品",29:"売上急増商品",30:"月別売上"};if(n===30){var mm={};raw.forEach(function(r){var mo=r["年月"]||"不明";if(!mm[mo])mm[mo]=[];mm[mo].push(r);});var d=srt(Object.keys(mm).filter(function(k){return k!=="不明";}).map(function(mo){var g=mm[mo];return{月:mo,件数:ssum(g,"件数"),推定売上:Math.round(ssum(g,"件数")*aavg(g,"平均単価")),推定利益:Math.round(ssum(g,"件数")*aavg(g,"推定利益/件")),平均単価:Math.round(aavg(g,"平均単価")),利益_件:Math.round(aavg(g,"推定利益/件"))};}),"月",false);var t=d[0]||{};return bk+krow([[t["月"]||"-","最新月"],[(t["件数"]||0).toLocaleString()+"件","件数"],[yen(t["推定売上"]),"推定売上"],[yen(t["推定利益"]),"推定利益"]])+"<div class='sec'>月別売上 全件</div><button class='bcsv' onclick='dlCSV(30)'>⬇ CSV</button>"+tbl(d,[{k:"__r",lb:"Rank"},{k:"月",lb:"月"},N.件数,{k:"推定売上",lb:"推定売上",f:yen,cl:"nr"},{k:"推定利益",lb:"推定利益",f:yen,cl:"nr"},N.単価,{k:"利益_件",lb:"利益/件",f:yen,cl:"nr"}]);}var d=(n===27)?srt(cd,"需要スコア",false):(n===28)?srt(bd,"平均単価",false):(n===29)?srt(bd,"仕入れスコア",false):srt(bd,"需要スコア",false);var t=d[0]||{};var kl0=(n===27)?[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">"):(t["ブランド"]||"-");var kv0=(n===28)?yen(t["平均単価"]):(t["件数"]||0).toLocaleString()+"件";return bk+krow([[kv0,"1位:"+kl0],[d.length.toLocaleString()+"件","集計件数"],[yen(t["平均単価"]),"1位平均単価"],[pct(t["利益率"]),"1位利益率"]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,BC.concat([N.件数,N.需要,N.単価,N.利益,N.利益率]));}
  return"<div class='nodata'>分析 #"+n+" 準備中</div>";
}
function genPurchase(){
  var raw=getFiltered();if(!raw.length)return"<div class='nodata'>絞込条件に一致するデータがありません</div>";
  var d=srt(grpBrand(raw),"仕入れスコア",false),t=d[0]||{},bk=badge();
  return bk+krow([[d.length.toLocaleString()+"件","対象件数"],[yen(Math.round(aavg(d,"平均単価"))),"平均単価"],[yen(Math.round(aavg(d,"推定利益"))),"平均推定利益/件"],[pct(parseFloat(aavg(d,"利益率").toFixed(1))),"平均利益率"]])+"<div class='ins'><h4>💡 ポイント</h4><ul><li>仕入れスコア=件数×平均単価×利益率</li><li>売却時間はDBから実計算（0日の場合は時間表示）</li></ul></div><div class='sec'>仕入れ推奨 全件（仕入れスコア順）</div><button class='bcsv' onclick='dlCSV(0)'>⬇ CSV</button>"+tbl(d,BC.concat([N.送料,N.件数,N.需要,N.単価,N.仕入,N.利益,N.利益率,N.売却日,N.即売,N.スコア]));
}
var CPOP=0;
function openPop(n){CPOP=n;var parts=fparts();document.getElementById("pnum").textContent="#"+n;document.getElementById("ptitle").textContent=CARDS[n-1]?CARDS[n-1].t:"";document.getElementById("pper").textContent="📅 "+CUR_FROM+" 〜 "+CUR_TO;var fb=document.getElementById("pfb");if(parts.length){fb.textContent="🔍 "+parts.join(" > ");fb.style.display="";}else fb.style.display="none";document.getElementById("pbody").innerHTML="<div style='text-align:center;padding:40px;color:#4a5568;'>⟳ 集計中...</div>";document.getElementById("ov").classList.add("on");setTimeout(function(){document.getElementById("pbody").innerHTML=genPop(n);},8);}
function openPurchase(){CPOP="P";var parts=fparts();document.getElementById("pnum").textContent="P";document.getElementById("ptitle").textContent="仕入れ推奨ランキング";document.getElementById("pper").textContent="📅 "+CUR_FROM+" 〜 "+CUR_TO;var fb=document.getElementById("pfb");if(parts.length){fb.textContent="🔍 "+parts.join(" > ");fb.style.display="";}else fb.style.display="none";document.getElementById("pbody").innerHTML="<div style='text-align:center;padding:40px;color:#4a5568;'>⟳ 集計中...</div>";document.getElementById("ov").classList.add("on");setTimeout(function(){document.getElementById("pbody").innerHTML=genPurchase();},8);}
function closePop(e){if(!e||e.target===document.getElementById("ov"))document.getElementById("ov").classList.remove("on");}
document.addEventListener("keydown",function(e){if(e.key==="Escape")document.getElementById("ov").classList.remove("on");});
function dlCSV(n){
  var raw=getFiltered(),d;
  if(n===0)d=srt(grpBrand(raw),"仕入れスコア",false);
  else if(n<=5)d=srt(grpBrand(raw),["","需要スコア","平均売却秒数","平均単価","平均売却秒数","推定利益"][n],[,false,true,false,true,false][n]);
  else if(n<=10)d=srt(grpCat(raw),["","","需要スコア","平均売却秒数","平均単価","平均売却秒数","推定利益"][n-5],[,,false,true,false,true,false][n-5]);
  else if(n<=15)d=srt(grpBrand(raw),"需要スコア",false);else d=raw;
  if(!d||!d.length){alert("データなし");return;}
  var NAMES={0:"仕入れ推奨ランキング",1:"ブランド売上ランキング",2:"ブランド回転率",3:"ブランド平均売却価格",4:"ブランド平均売却時間",5:"ブランド利益率",6:"カテゴリ売上ランキング",7:"カテゴリ回転率",8:"カテゴリ平均価格",9:"カテゴリ平均売却時間",10:"カテゴリ利益率",11:"売れる商品ランキング",12:"商品回転率",13:"商品平均売却価格",14:"商品平均売却時間",15:"商品利益率",16:"売れる価格帯",17:"価格帯別回転率",18:"価格帯別利益率",19:"平均値下げ幅",20:"値下げ売却率",21:"タイトルKW",22:"検索ヒットKW",23:"ブランド商品名成約率",24:"型番効果",25:"送料表記効果",26:"急上昇ブランド",27:"急上昇カテゴリ",28:"価格上昇商品",29:"売上急増商品",30:"月別売上"};
  var nm=NAMES[n]||("分析"+n);
  if(!window._dlVol)window._dlVol={};window._dlVol[nm]=(window._dlVol[nm]||0)+1;
  var fn=nm+"_Vol"+window._dlVol[nm]+".csv";
  var keys=Object.keys(d[0]);var bom="\uFEFF";
  var csv=bom+[keys].concat(d.map(function(r){return keys.map(function(k){var v=String(r[k]===undefined?"":r[k]);if(v.indexOf(",")>=0||v.indexOf('"')>=0)v='"'+v.replace(/"/g,'""')+'"';return v;});})).map(function(r){return r.join(",");}).join("\n");
  try{var blob=new Blob([csv],{type:"text/csv;charset=utf-8"});var url=URL.createObjectURL(blob);var a=document.createElement("a");a.href=url;a.download=fn;a.style.display="none";document.body.appendChild(a);a.click();setTimeout(function(){URL.revokeObjectURL(url);document.body.removeChild(a);},100);}
  catch(e2){var a=document.createElement("a");a.href="data:text/csv;charset=utf-8,"+encodeURIComponent(csv);a.download=fn;a.style.display="none";document.body.appendChild(a);a.click();setTimeout(function(){document.body.removeChild(a);},100);}
}
function showTab(n){document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("on");});document.querySelectorAll("nav button").forEach(function(b){b.classList.remove("on");});document.getElementById("tab"+n).classList.add("on");document.querySelectorAll("nav button")[n-1].classList.add("on");}
var CARDS=[
  {n:1,t:"ブランド売上ランキング",tab:1,d:"需要スコア降順"},{n:2,t:"ブランド回転率",tab:1,d:"売却時間昇順"},
  {n:3,t:"ブランド平均売却価格",tab:1,d:"平均単価降順"},{n:4,t:"ブランド平均売却時間",tab:1,d:"売却時間昇順"},
  {n:5,t:"ブランド利益率",tab:1,d:"推定利益/件降順"},{n:6,t:"カテゴリ売上ランキング",tab:2,d:"需要スコア降順"},
  {n:7,t:"カテゴリ回転率",tab:2,d:"売却時間昇順"},{n:8,t:"カテゴリ平均価格",tab:2,d:"平均単価降順"},
  {n:9,t:"カテゴリ平均売却時間",tab:2,d:"売却時間昇順"},{n:10,t:"カテゴリ利益率",tab:2,d:"推定利益降順"},
  {n:11,t:"売れる商品ランキング",tab:3,d:"需要スコア降順"},{n:12,t:"商品回転率",tab:3,d:"売却時間昇順"},
  {n:13,t:"商品平均売却価格",tab:3,d:"平均単価降順"},{n:14,t:"商品平均売却時間",tab:3,d:"売却時間昇順"},
  {n:15,t:"商品利益率",tab:3,d:"推定利益降順"},{n:16,t:"売れる価格帯ランキング",tab:4,d:"件数降順"},
  {n:17,t:"価格帯別回転率",tab:4,d:"売却時間昇順"},{n:18,t:"価格帯別利益率",tab:4,d:"利益率降順"},
  {n:19,t:"平均値下げ幅",tab:4,d:"価格帯別分析"},{n:20,t:"値下げ回数と売却率",tab:4,d:"価格帯別分析"},
  {n:21,t:"タイトルKWランキング",tab:5,d:"出現頻度降順"},{n:22,t:"検索ヒットKW",tab:5,d:"キーワード分析"},
  {n:23,t:"ブランド+商品名成約率",tab:5,d:"ブランド×カテゴリ"},{n:24,t:"型番あり vs なし",tab:5,d:"型番効果分析"},
  {n:25,t:"送料無料表記効果",tab:5,d:"送料別分析"},{n:26,t:"急上昇ブランド",tab:6,d:"需要スコア上位"},
  {n:27,t:"急上昇カテゴリ",tab:6,d:"カテゴリ需要スコア上位"},{n:28,t:"価格上昇商品",tab:6,d:"平均単価上位"},
  {n:29,t:"売上急増商品",tab:6,d:"仕入れスコア上位"},{n:30,t:"月別売上ランキング",tab:6,d:"月別集計"}
];
function buildGrids(){
  for(var tab=1;tab<=7;tab++){
    var el=document.getElementById("g"+tab);if(!el)continue;var h="";
    if(tab===7)h='<div class="card" onclick="openPurchase()" style="border-color:#f0b429;background:linear-gradient(135deg,#fff9e6,#fff);"><div class="ctop"><span class="cnum" style="background:#e67700;">P</span><span class="ctitle" style="font-size:13px;">🏆 仕入れ推奨ランキング</span></div><div class="cdesc">仕入れスコアで絞込後データから総合評価</div></div>';
    CARDS.filter(function(c){return c.tab===tab;}).forEach(function(c){h+='<div class="card" onclick="openPop('+c.n+')"><div class="ctop"><span class="cnum">#'+c.n+'</span><span class="ctitle">'+c.t+'</span></div><div class="cdesc">'+c.d+'</div></div>';});
    el.innerHTML=h;
  }
}
document.addEventListener("DOMContentLoaded",function(){
  var today=new Date();
  var ymd=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0")+"-"+String(today.getDate()).padStart(2,"0");
  document.getElementById("dfrom").value=DB_START;
  document.getElementById("dto").value=ymd;
  CUR_FROM=DB_START;CUR_TO=ymd;
  initSel();buildGrids();refreshUI();
});
