import json
import os
import glob

def combine():
    # 実行しているカレントディレクトリ（htmlフォルダ）のJSONを探す
    json_files = glob.glob("mercari_*_data.json")
    all_data = []

    print(f"--- 11万件の統合を開始します ---")

    for file in json_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                content = json.load(f)
                if isinstance(content, list):
                    all_data.extend(content)
                elif isinstance(content, dict):
                    for val in content.values():
                        if isinstance(val, list):
                            all_data.extend(val)
                            break
            print(f"読込完了: {file}")
        except Exception as e:
            print(f"エラー: {file} ({e})")

    total = len(all_data)
    print(f"\n--- 最終集計: {total} 件 ---")

    # 書き出し先を「現在のフォルダの data.js」に強制指定
    target_path = os.path.join(os.getcwd(), "data.js")
    
    print(f"書き出し中... (ファイルサイズが大きいため数十秒かかる場合があります)")
    
    with open(target_path, 'w', encoding='utf-8') as f:
        # JS用ヘッダー
        f.write("const CAT = " + json.dumps({"ファッション":{"メンズ":{},"レディース":{}}}, ensure_ascii=False) + ";\n")
        # RAWデータの書き出し
        f.write("const RAW = " + json.dumps(all_data, ensure_ascii=False) + ";\n")

    # 物理的なファイルサイズを確認
    size_mb = os.path.getsize(target_path) / (1024 * 1024)
    print(f"成功!! {target_path} を作成しました。")
    print(f"物理サイズ: {size_mb:.2f} MB")

if __name__ == "__main__":
    combine()