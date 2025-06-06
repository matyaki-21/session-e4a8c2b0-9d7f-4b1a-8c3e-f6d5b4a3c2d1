import os
import pandas as pd
import fnmatch
import chardet
import logging

def detect_encoding(file_path):
    """
    ファイルのエンコーディングを検出するヘルパー関数
    """
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  # 最初の10KBを検査
    return result['encoding']

def is_text_file(file_path):
    """
    ファイルがテキストファイルかどうかを判定するヘルパー関数。
    Excelファイルのシグネチャを確認して区別する。
    """
    try:
        with open(file_path, 'rb') as file:
            header = file.read(8)
        if header.startswith(b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1') or header.startswith(b'PK'):
            return False  # Excelファイル
        else:
            return True  # テキストファイル
    except:
        return False

def get_previous_year_month(processing_year_month):
    """
    processing_year_monthの前月を計算するヘルパー関数。
    Args:
        processing_year_month (int): 処理年月（YYYYMM形式）
    Returns:
        int: 前月の年月（YYYYMM形式）
    """
    year = processing_year_month // 100
    month = processing_year_month % 100
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    return year * 100 + month

def insert_year_month_column(input_dir_path, output_dir_path, processing_year_month, get_sheet_name_func, exclude_files=('*SBU別売上債権（各種売掛金）*','*原価差額調整計算表 AI.xlsx','人員集計表*','受注売上受注残*','*品目別在庫明細表.xlsx','PJ管理物件勘定内訳表*','*建仮計上額.xls')):
    """
    Excelファイルまたはテキストファイルの先頭に「年月」列を追加し、TMPフォルダに保存する関数。
    Args:
        input_dir_path (str): 入力ディレクトリのパス
        output_dir_path (str): 出力ディレクトリのパス
        processing_year_month (int): 処理年月（YYYYMM形式）
        get_sheet_name_func (function): ファイル名に基づいてシート名を取得する関数
        exclude_files (tuple): 「年月」列を追加しないファイル名のリスト
    """
    
    logging.info(f"ディレクトリ {input_dir_path} 内のExcelファイル変換処理を開始します。")

    # TMPフォルダが存在しない場合は作成
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
        logging.info(f"出力ディレクトリ {output_dir_path} を作成しました。")

    # フォルダ内のすべてのExcelファイルまたはテキストファイルを処理
    for filename in os.listdir(input_dir_path):
        # 対応する拡張子のチェック
        if filename.lower().endswith(('.xlsx', '.xls')):
            # ファイルパスの生成
            file_path = os.path.join(input_dir_path, filename)
            logging.info(f"ファイル {filename} の処理を開始します。")
            sheet_name = get_sheet_name_func(filename)  # ファイル名に基づくシート名取得

            # ファイル名に基づいて、前月を埋め込むか現月を埋め込むかを決定
            if fnmatch.fnmatch(filename, '品目一覧表.xls') or fnmatch.fnmatch(filename, '*AQZZCO_CT.xlsx'):
                year_month_to_insert = get_previous_year_month(processing_year_month)
            else:
                year_month_to_insert = processing_year_month

            # テキストファイルかどうかを判定
            if is_text_file(file_path) and filename.lower().endswith('.xls'):
                
                # エンコーディングを自動検出
                #encoding = detect_encoding(file_path)
                # エンコーディングを'cp932'で固定
                encoding = 'cp932'
                logging.info(f"エンコーディングを固定します。 {filename}: cp932")
                
                # テキストファイルの場合はread_csvで読み込む
                try:
                    df = pd.read_csv(file_path, sep='\t', encoding=encoding)  # セパレータを適宜設定
                except (UnicodeDecodeError, ValueError):
                    logging.warning(f"{file_path} の処理中にエラーが発生。エンコーディング変更して再試行。")

                    # エンコーディングを自動検出
                    detected_encoding = detect_encoding(file_path)
                    logging.info(f"{file_path} の検出エンコーディング: {detected_encoding}")

                    # ファイルを1行ずつ読み込み、データ行を判定して抽出
                    try:
                        valid_rows = []  # データ部分のみ格納するリスト

                        with open(file_path, 'r', encoding=detected_encoding) as f:
                            for line in f:
                                # 空白のみの行はスキップ
                                if line.strip() == "":
                                    continue

                                # ヘッダー行のパターン（タイトルやページ情報を除外）
                                if "ナブテスコ株式会社" in line or "Japan 元帳" in line or "ページ" in line:
                                    continue
                                if "会社コード" in line and "事業領域" in line and "金額" in line:
                                    continue
                                if "C 会社 事業 テキスト" in line:
                                    continue
                                if "＜" in line and "＞" in line:  # 「＜資産の部＞」などのタイトル行を除外
                                    continue

                                # 有効なデータ行を追加
                                valid_rows.append(line)

                        # 抽出したデータをDataFrameに変換（タブ区切りのみ適用）
                        df = pd.DataFrame([row.strip().split('\t') for row in valid_rows])

                        # 空の列を削除
                        df.dropna(axis=1, how='all', inplace=True)

                        logging.info(f"{file_path} のデータ処理が完了しました。")

                    except Exception as e:
                        logging.error(f"{file_path} の読み込みに最終的に失敗しました: {e}")
                        raise
                    
            else:
                # Excelファイルの場合はread_excelで読み込む
                sheet_name = get_sheet_name_func(filename)  # ファイル名に基づくシート名取得
                if sheet_name:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                else:
                    df = pd.read_excel(file_path)
                
            # 除外ファイルのチェック
            if not any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_files):
                # 除外ファイルでない場合は「年月」列を追加
                df.insert(0, '年月', year_month_to_insert)

            # ファイル名を拡張子を含まずに取得し、.xlsxに変更
            output_filename = os.path.splitext(filename)[0] + '.xlsx'
            output_file_path = os.path.join(output_dir_path, output_filename)

            # 処理後のファイルをTMPフォルダに.xlsx形式で保存
            df.to_excel(output_file_path, index=False)

    logging.info(f"処理が完了しました。")
