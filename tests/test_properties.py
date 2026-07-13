from chem_pipeline.properties import calc_properties

def test_props():
    p = calc_properties("CCO")
    assert p["HBD"] == 1 and p["HBA"] == 1
