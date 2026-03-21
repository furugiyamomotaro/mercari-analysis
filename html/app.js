var RAW = RAW || [];

function init() {
    const sbar = document.getElementById("sbar");
    const sl1 = document.getElementById("sl1");
    const sl2 = document.getElementById("sl2");

    if (!RAW || RAW.length === 0) {
        sbar.innerHTML = "データがありません(data.jsを確認してください)";
        return;
    }

    // 項目名の自動判定 (L1 or カテゴリ1)
    const k1 = "L1" in RAW[0] ? "L1" : "カテゴリ1";
    const k2 = "L2" in RAW[0] ? "L2" : "カテゴリ2";

    // L1プルダウン生成
    const l1List = [...new Set(RAW.map(i => i[k1]))].filter(Boolean).sort();
    sl1.innerHTML = "<option value=''>- L1選択 -</option>";
    l1List.forEach(v => {
        let opt = document.createElement("option");
        opt.value = v; opt.textContent = v;
        sl1.appendChild(opt);
    });

    // L1変更イベント
    sl1.onchange = function() {
        const val1 = this.value;
        const filteredL2 = RAW.filter(i => i[k1] === val1);
        const l2List = [...new Set(filteredL2.map(i => i[k2]))].filter(Boolean).sort();
        
        sl2.innerHTML = "<option value=''>- L2選択 -</option>";
        l2List.forEach(v => {
            let opt = document.createElement("option");
            opt.value = v; opt.textContent = v;
            sl2.appendChild(opt);
        });
        sl2.disabled = false;
        render(filteredL2);
    };

    // L2変更イベント
    sl2.onchange = function() {
        const finalData = RAW.filter(i => i[k1] === sl1.value && i[k2] === this.value);
        render(finalData);
    };

    render(RAW);
}

function render(data) {
    document.getElementById("sbar").innerHTML = "全 " + RAW.length.toLocaleString() + " 件 / 抽出: " + data.length.toLocaleString() + " 件";
    const container = document.getElementById("grid-container");
    // 描画負荷を抑えるため先頭100件のみ表示
    container.innerHTML = data.slice(0, 100).map(i => `
        <div class="card">
            <b>¥${(i.価格 || 0).toLocaleString()}</b><br>
            ${i.ブランド || '不明'}<br>
            ${(i.タイトル || '').substring(0, 20)}
        </div>
    `).join('');
}

window.onload = init;