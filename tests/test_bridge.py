import meshtastic_discord_bridge as bridge


# ---------------------------------------------------------------------------
# parse_position
# ---------------------------------------------------------------------------

def test_parse_position_float_fields():
    node = {'position': {'latitude': 37.7749, 'longitude': -122.4194, 'altitude': 42}}
    lat, lon, alt = bridge.parse_position(node)
    assert abs(lat - 37.7749) < 1e-6
    assert abs(lon - (-122.4194)) < 1e-6
    assert alt == 42


def test_parse_position_integer_fields():
    node = {'position': {'latitudeI': 377749000, 'longitudeI': -1224194000}}
    lat, lon, alt = bridge.parse_position(node)
    assert abs(lat - 37.7749) < 1e-4
    assert abs(lon - (-122.4194)) < 1e-4
    assert alt is None


def test_parse_position_missing_position_key():
    assert bridge.parse_position({}) == (None, None, None)


def test_parse_position_empty_position_dict():
    assert bridge.parse_position({'position': {}}) == (None, None, None)


# ---------------------------------------------------------------------------
# find_node
# ---------------------------------------------------------------------------

SAMPLE_NODES = [
    {
        'id': '!abc123', 'num': '111222333', 'longname': 'Alpha',
        'hopsaway': '0', 'snr': '-5.0', 'lastheardutc': '2026-04-29 12:00:00',
        'ts': 1745928000, 'lat': 37.7749, 'lon': -122.4194, 'alt': 10,
    },
    {
        'id': '!def456', 'num': '444555666', 'longname': 'Beta',
        'hopsaway': '1', 'snr': 'N/A', 'lastheardutc': 'Never',
        'ts': 0, 'lat': None, 'lon': None, 'alt': None,
    },
]


def test_find_node_by_hex_id():
    assert bridge.find_node(SAMPLE_NODES, '!abc123')['longname'] == 'Alpha'


def test_find_node_by_hex_id_case_insensitive():
    assert bridge.find_node(SAMPLE_NODES, '!ABC123')['longname'] == 'Alpha'


def test_find_node_by_num():
    assert bridge.find_node(SAMPLE_NODES, '444555666')['longname'] == 'Beta'


def test_find_node_not_found():
    assert bridge.find_node(SAMPLE_NODES, '!zzz999') is None
