from  multitudcsd.synthetic.mentions import generate_mentions

def test_los_identificadores_no_se_repiten_entre_lotes():
    """mention_id es la clave de deduplicacion de Silver: tiene que ser unico en el conjunto."""
    lote_1 = generate_mentions(10, seed=1, primer_num=1)
    lote_2 = generate_mentions(10, seed=2, primer_num=11)

    identificadores = [m["mention_id"] for m in lote_1 + lote_2]
    assert len(set(identificadores)) == 20