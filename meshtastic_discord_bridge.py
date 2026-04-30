import discord
import asyncio
import os
import sys
import io
from dotenv import load_dotenv
from pubsub import pub
import meshtastic
import meshtastic.tcp_interface
import meshtastic.serial_interface
import queue
import time
from datetime import datetime
import threading
from flask import Flask, jsonify

load_dotenv()
token = os.getenv("DISCORD_TOKEN")
channel_id = int(os.getenv("DISCORD_CHANNEL_ID"))
secondary_channel_id = os.getenv("DISCORD_SECONDARY_CHANNEL_ID")
secondary_channel_id = int(secondary_channel_id) if secondary_channel_id else None
primary_mesh_channel_index = int(os.getenv("MESHTASTIC_PRIMARY_CHANNEL_INDEX", "0"))
meshtastic_hostname = os.getenv("MESHTASTIC_HOSTNAME")


def parse_position(node):
    pos = node.get('position') or {}
    lat = pos.get('latitude')
    lon = pos.get('longitude')
    if lat is None and 'latitudeI' in pos:
        lat = pos['latitudeI'] * 1e-7
    if lon is None and 'longitudeI' in pos:
        lon = pos['longitudeI'] * 1e-7
    if lat is None or lon is None:
        return None, None, None
    return lat, lon, pos.get('altitude')


def find_node(nodes_snapshot, query):
    query = query.strip()
    for node in nodes_snapshot:
        if node['id'].lower() == query.lower():
            return node
        if node['num'] == query:
            return node
    return None


meshtodiscord = queue.Queue()
discordtomesh = queue.Queue()
nodelistq = queue.Queue()
all_nodes = []
nodes_lock = threading.Lock()

def onConnectionMesh(interface, topic=pub.AUTO_TOPIC):  
    """called when we (re)connect to the meshtastic radio"""
    print(interface.myInfo)

def onReceiveMesh(packet, interface):  
    """called when a packet arrives from mesh"""
    try:
        if 'decoded' in packet: 
            if packet['decoded']['portnum']=='TEXT_MESSAGE_APP': #only interest in text packets for now
                from_id = str(packet.get('fromId', 'unknown'))
                to_id = str(packet.get('toId', 'unknown'))
                text = str(packet['decoded'].get('text', ''))
                longname = from_id
                try:
                    for node in interface.nodes.values():
                        if str(node.get('user', {}).get('id', '')) == from_id:
                            longname = str(node.get('user', {}).get('longName', from_id))
                            break
                except Exception:
                    pass
                meshtodiscord.put({
                    'longname': longname,
                    'node_id': from_id,
                    'to_id': to_id,
                    'channel_index': packet.get('channel'),
                    'text': text,
                })
#    App was occasionally failing where packet['fromId'] was nonetype, let's see if catching all exceptions helps
#    except KeyError as e: #catch empty packet
#        pass
    except Exception as e:
        print("On receive mesh exception: " + str(e))
        
class MyClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def setup_hook(self) -> None:
        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')

    async def on_connection(self):  # pylint: disable=unused-argument
        # nothing to do here
        return


    async def on_message(self, message):
        if message.author.id == self.user.id:
            return

        if message.content.startswith('$help'):
            helpmessage="Meshtastic Discord Bridge is up.  Command list:\n"\
                "$sendprimary <message> sends a message up to 225 characters to the the primary channel\n"\
                "$send nodenum=########### <message> sends a message up to 225 characters to nodenum ###########\n"\
                "$activenodes will list all nodes seen in the last 15 minutes\n"\
                "Set DISCORD_SECONDARY_CHANNEL_ID to forward non-primary mesh messages to another Discord channel"
            await message.channel.send(helpmessage)

        if message.content.startswith('$sendprimary'):
            tempmessage=str(message.content)
            tempmessage=tempmessage[tempmessage.find(' ')+1:225] #could be 228
            await message.channel.send('Sending the following message to the primary channel:\n'+tempmessage)
            discordtomesh.put(tempmessage)

        if message.content.startswith('$send nodenum='):
            tempmessage=str(message.content)
            nodenumstr=tempmessage[14:tempmessage.find(' ',14)+1]
            tempmessage=tempmessage[tempmessage.find(' ',14)+1:225] #could be 228
            try:
                nodenum=int(nodenumstr)
                await message.channel.send('Sending the following message:\n'+tempmessage+'\nto nodenum:\n'+str(nodenum))
                discordtomesh.put("nodenum="+str(nodenum)+ " "+tempmessage)
            except:
                await message.channel.send('Could not send message')
                 
 

        if message.content.startswith('$activenodes'):
            nodelistq.put("just pop a message on this queue so we know to send nodelist to discord")


    async def my_background_task(self):
        await self.wait_until_ready()
        counter = 0
        primary_channel = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
        secondary_channel = None
        if secondary_channel_id:
            secondary_channel = self.get_channel(secondary_channel_id)
            if secondary_channel is None:
                try:
                    secondary_channel = await self.fetch_channel(secondary_channel_id)
                except Exception:
                    secondary_channel = None
        print(
            "Routing config: "
            + f"primary_discord_channel={channel_id}, "
            + f"secondary_discord_channel={secondary_channel_id}, "
            + f"primary_mesh_channel_index={primary_mesh_channel_index}"
        )
        if secondary_channel_id and secondary_channel is None:
            print("Warning: DISCORD_SECONDARY_CHANNEL_ID is set, but bot could not access that channel. Falling back to primary channel.")
        pub.subscribe(onReceiveMesh, "meshtastic.receive")
        pub.subscribe(onConnectionMesh, "meshtastic.connection.established")
        try:
            if len(meshtastic_hostname)>1:
                print("Trying TCP interface to "+meshtastic_hostname)
                iface = meshtastic.tcp_interface.TCPInterface(meshtastic_hostname)
            else:
                print("Trying serial interface")
                iface =  meshtastic.serial_interface.SerialInterface()
        except Exception as ex:
            print(f"Error: Could not connect {ex}")
            sys.exit(1)
        while not self.is_closed():
            counter += 1
            #Helpful to uncomment this print counter if you need to know if this task is still running
            #print(counter)
            if (counter%12==1):
                global all_nodes
                iface_nodes = iface.nodes
                new_nodes = []
                for node in iface_nodes:
                    try:
                        node_id = str(iface_nodes[node]['user']['id'])
                        num = str(iface_nodes[node]['num'])
                        longname = str(iface_nodes[node]['user']['longName'])
                        hopsaway = str(iface_nodes[node].get('hopsAway', 0))
                        snr = str(iface_nodes[node]['snr']) if 'snr' in iface_nodes[node] else 'N/A'
                        if 'lastHeard' in iface_nodes[node]:
                            ts = int(iface_nodes[node]['lastHeard'])
                            timestr = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            ts = 0
                            timestr = 'Never'
                        lat, lon, alt = parse_position(iface_nodes[node])
                        new_nodes.append({
                            'id': node_id,
                            'num': num,
                            'longname': longname,
                            'hopsaway': hopsaway,
                            'snr': snr,
                            'lastheardutc': timestr,
                            'ts': ts,
                            'lat': lat,
                            'lon': lon,
                            'alt': alt,
                        })
                    except KeyError as e:
                        print(e)
                with nodes_lock:
                    all_nodes = new_nodes
            try:
                meshmessage=meshtodiscord.get_nowait()
                channel_index = meshmessage.get('channel_index')
                try:
                    channel_index = int(channel_index) if channel_index is not None else None
                except (TypeError, ValueError):
                    channel_index = None

                is_primary_mesh_channel = channel_index == primary_mesh_channel_index if channel_index is not None else meshmessage['to_id'] == '^all'
                channel = primary_channel if is_primary_mesh_channel else secondary_channel
                if channel is None:
                    channel = primary_channel
                route_name = "primary"
                if channel == secondary_channel and secondary_channel is not None:
                    route_name = "secondary"
                print(
                    "Routing incoming mesh message: "
                    + f"from={meshmessage['node_id']}, "
                    + f"to_id={meshmessage['to_id']}, "
                    + f"channel_index={channel_index}, "
                    + f"route={route_name}"
                )
                msg = (
                    f"📻 **{meshmessage['longname']}** (`{meshmessage['node_id']}`) → `{meshmessage['to_id']}`"
                    + (f" (ch {channel_index})" if channel_index is not None else "")
                    + "\n"
                    f"{meshmessage['text']}"
                )
                await channel.send(msg)
                meshtodiscord.task_done()
            except queue.Empty:
                pass
            try:
                meshmessage=discordtomesh.get_nowait()
                if meshmessage.startswith('nodenum='):
                    nodenum=int(meshmessage[8:meshmessage.find(' ')])
                    iface.sendText(meshmessage[meshmessage.find(' ')+1:],destinationId=nodenum)
                else:    
                    iface.sendText(meshmessage)
                discordtomesh.task_done()
            except: #lets pass on both the empty queue and the int conversion
                pass
            try:
                nodelistq.get_nowait()
                #if there's any item on this queue, we'll send the node list as embeds
                if not active_nodes:
                    await primary_channel.send(embed=discord.Embed(
                        title="🟢 Active Nodes",
                        description="No active nodes found in the last 15 minutes.",
                        color=0x00ff99,
                    ))
                else:
                    for offset in range(0, len(active_nodes), 25):
                        embed = discord.Embed(title="🟢 Active Nodes", color=0x00ff99)
                        for node in active_nodes[offset:offset + 25]:
                            embed.add_field(
                                name=f"📡 {node['longname']} (`{node['id']}`)",
                                value=f"SNR: `{node['snr']}` | Last heard: `{node['lastheardutc']}`",
                                inline=False,
                            )
                        await primary_channel.send(embed=embed)
                nodelistq.task_done()
            except queue.Empty:
                pass
            await asyncio.sleep(5) 
        
if __name__ == '__main__':
    start_map_server()
    intents = discord.Intents.default()
    intents.message_content = True
    client = MyClient(intents=intents)
    client.run(token)
