# meshtastic_discord_bridge

A Discord bot which bridges discussions between a Discord channel and a Meshtastic mesh through a locally connected radio. Also hosts a live web bridge with a node map and mesh messaging on your LAN.

## Requirements

- Python 3
- A supported Meshtastic radio connected via USB or TCP
- Discord bot token and channel ID

## Installation and Startup

Get a Discord Bot account and invite the bot to a server. [Instructions](https://discordpy.readthedocs.io/en/stable/discord.html)

Fill in the values for your environment in `sampledotenvfile` and rename it to `.env`.

If you want private or non-primary Meshtastic messages to go to a separate Discord channel, set `DISCORD_SECONDARY_CHANNEL_ID` as well.
By default, mesh channel index `0` is treated as primary. You can override that with `MESHTASTIC_PRIMARY_CHANNEL_INDEX`.

If you connect to your mesh device via TCP, specify the hostname in `MESHTASTIC_HOSTNAME`. If no hostname is specified, a serial interface is assumed.

```
python3 -m pip install -r requirements.txt
python meshtastic_discord_bridge.py
```

## Discord Commands

| Command | Description |
|---------|-------------|
| `$sendprimary <message>` | Send a message (up to 225 characters) to the primary Meshtastic channel |
| `$send nodenum=########### <message>` | Send a message to a specific node by number |
| `$activenodes` | List all nodes heard in the last 15 minutes, including coordinates if available |
| `$nodeinfo <id>` | Show full details for a node — accepts hex ID (e.g. `!abc123`) or node number |
| `$help` | Show the command list |

### `$activenodes`

Returns an embed listing all nodes heard in the last 15 minutes with SNR, last heard time, and GPS coordinates (latitude, longitude, and altitude) when available.

### `$nodeinfo <id>`

Returns a full detail embed for a single node:

- Node name and hex ID
- Numeric node number
- Hops away
- SNR
- Last heard (UTC)
- Position (lat/lon/alt, or "Not available")

Accepts either the hex node ID (`!abc123`) or the numeric node number. The hex ID lookup is case-insensitive.

## LAN Web Bridge

When the bridge is running, a live web bridge is served at:

```
http://<your-host>:8765
```

- Nodes with GPS position are shown as markers on an OpenStreetMap map (no API key required)
- Click any marker to see a popup with full node details
- Nodes without position data are listed in a sidebar
- Incoming mesh text messages are shown in the message feed
- Send messages to the primary mesh channel or directly to a node number
- The map auto-refreshes every 5 seconds — no page reload needed
- The message feed auto-refreshes every 3 seconds

## Channel Routing

If `DISCORD_SECONDARY_CHANNEL_ID` is set, incoming Meshtastic messages not on `MESHTASTIC_PRIMARY_CHANNEL_INDEX` are forwarded to the secondary Discord channel instead of the primary one. If a packet has no channel index, the bridge falls back to routing by destination (`^all` goes to primary, everything else to secondary).

## Screenshot

![Interacting with Meshtastic through Discord](/DiscordScreenshot.png)
