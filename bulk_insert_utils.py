from sqlalchemy.orm import Session
import logging

def bulk_insert_with_chunk(session: Session, model, data_list, chunk_size=1000):
    """
    データをチャンク単位でバルクインサートする共通関数。

    Args:
        session (Session): SQLAlchemyセッション
        model: SQLAlchemyのモデルクラス
        data_list (List[Dict]): 挿入対象の辞書形式データ
        chunk_size (int): 1チャンクあたりのレコード数（デフォルト1000）
    """
    try:
        for i in range(0, len(data_list), chunk_size):
            chunk = data_list[i:i + chunk_size]
            session.bulk_insert_mappings(model, chunk)
        session.commit()
        logging.info(f"{model.__tablename__} に {len(data_list)} 件のデータをバルクインサートしました。")
    except Exception as e:
        session.rollback()
        logging.error(f"{model.__tablename__} へのデータ挿入中にエラー: {e}")
        raise
