import json
import os
import glob

def combine_all_json_to_js():
    # 1. フォルダ内の「mercari_」で始まる全JSONファイルを取得
    json_files = glob.glob("mercari_*_data.json")
    all_raw_data = []

    print(f"検証: {len(json_files)} 個のデータファイルを発見しました。")

    for file in json_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # リスト形式ならそのまま統合、単体なら追加
                if isinstance(data, list):
                    all_raw_data.extend(data)
                else:
                    all_raw_data.append(data)
                print(f"  読み込み成功: {file} ({len(data)}件)")
        except Exception as e:
            print(f"  エラー回避: {file} の読み込みに失敗しました。")

    # 2. data.js の作成
    with open('data.js', 'w', encoding='utf-8') as f:
        f.write("// ==========================================\n")
        f.write(f"// data.js - 全カテゴリ統合版 (合計 {len(all_raw_data)} 件)\n")
        f.write("// ==========================================\n\n")
        
        # カテゴリ定義（ここはお客様のCAT構造に合わせてください）
        # L2に「メンズ」「レディース」がデータ内にあることを前提としています
        cat_struct = {
            "ファッション": {
                "メンズ": {"パンツ": {}, "トップス": {}, "ジャケット": {}, "バッグ": {}, "アクセサリー": {}},
                "レディース": {"パンツ": {}, "トップス": {}, "ジャケット": {}, "バッグ": {}, "アクセサリー": {}}
            }
        }
        f.write(f"const CAT = {json.dumps(cat_struct, ensure_ascii=False, indent=2)};\n\n")
        
        # 全データを RAW 配列として書き出し
        f.write("const RAW = " + json.dumps(all_raw_data, ensure_ascii=False) + ";\n")

    print(f"\n完了!! 全 {len(all_raw_data)} 件を統合して data.js を生成しました。")

if __name__ == "__main__":
    combine_all_json_to_js()