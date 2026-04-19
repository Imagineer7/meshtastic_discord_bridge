# meshtastic_discord_bridge

A Discord bot which bridges discussions between a Discord channel and a Meshtastic mesh through a locally connected radio

## Requirements

- Python
- A supported Meshtastic radio connected via USB 
- Discord key and a channel

## Installation and Startup

Get a Discord Bot account and invite the bot to a server.  [Instructions](https://discordpy.readthedocs.io/en/stable/discord.html)

Fill in the values for your environment in sampledotenvfile, and rename to .env 

If you want private or non-primary Meshtastic messages to go to a separate Discord channel, set `DISCORD_SECONDARY_CHANNEL_ID` as well.
By default, mesh channel index `0` is treated as primary. You can override that with `MESHTASTIC_PRIMARY_CHANNEL_INDEX`.

If you connect to your mesh device via TCP, specify the hostname in MESHTASTIC_HOSTNAME.  If no hostname is specified, a serial interface is assumed.

```
python3 -m pip install -r requirements.txt
python meshtastic_discord_bridge.py
```

## Usage

You can now interact with Meshtastic through Discord.

```
$sendprimary <message> sends a message up to 225 characters to the the primary channel
$send nodenum=########### <message> sends a message up to 225 characters to nodenum ###########
$activenodes will list all nodes seen in the last 15 minutes

If `DISCORD_SECONDARY_CHANNEL_ID` is set, incoming messages not on `MESHTASTIC_PRIMARY_CHANNEL_INDEX` are forwarded there instead of the primary Discord channel.
If the packet has no channel index, the bridge falls back to routing by destination (`^all` to primary, everything else to secondary).
```

## Screenshot

![Interacting with Meshtastic through Discord](/DiscordScreenshot.png)

