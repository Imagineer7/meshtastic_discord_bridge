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
from datetime import datetime, timezone
import threading
from flask import Flask, jsonify, request

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
mesh_messages = []
messages_lock = threading.Lock()
MAX_WEB_MESSAGES = 100


def add_mesh_message(message):
    with messages_lock:
        message = dict(message)
        message['id'] = len(mesh_messages) + 1
        message['timestamputc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        mesh_messages.append(message)
        del mesh_messages[:-MAX_WEB_MESSAGES]
        return message


def format_outbound_mesh_message(text, destination=None):
    if destination is None:
        return text
    return "nodenum=" + str(destination) + " " + text

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
                meshmessage = {
                    'longname': longname,
                    'node_id': from_id,
                    'to_id': to_id,
                    'channel_index': packet.get('channel'),
                    'text': text,
                }
                meshtodiscord.put(meshmessage)
                add_mesh_message({
                    **meshmessage,
                    'direction': 'inbound',
                    'source': 'mesh',
                })
#    App was occasionally failing where packet['fromId'] was nonetype, let's see if catching all exceptions helps
#    except KeyError as e: #catch empty packet
#        pass
    except Exception as e:
        print("On receive mesh exception: " + str(e))
        
MAP_HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Meshtastic Web Bridge</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      display: grid; grid-template-columns: minmax(0, 1fr) 360px; height: 100vh;
      font-family: Arial, sans-serif; background: #10141f; color: #f3f4f6;
    }
    #map { min-width: 0; }
    #sidebar {
      display: flex; flex-direction: column; min-width: 0; overflow: hidden;
      background: #151b26; border-left: 1px solid #2f3a4b;
    }
    .panel { padding: 12px; border-bottom: 1px solid #2f3a4b; }
    .panel:last-child { border-bottom: 0; }
    h2 {
      font-size: 0.78rem; color: #5eead4; margin-bottom: 8px;
      text-transform: uppercase; letter-spacing: 0.08em;
    }
    label { display: block; margin-bottom: 5px; color: #cbd5e1; font-size: 0.78rem; }
    textarea, input, select {
      width: 100%; border: 1px solid #3e4a5d; border-radius: 6px;
      background: #0f1724; color: #f8fafc; padding: 8px; font: inherit;
    }
    textarea { min-height: 76px; resize: vertical; }
    select, input { height: 36px; }
    button {
      width: 100%; height: 38px; border: 0; border-radius: 6px;
      background: #14b8a6; color: #042f2e; font-weight: 700; cursor: pointer;
    }
    button:disabled { opacity: 0.55; cursor: not-allowed; }
    .field { margin-bottom: 10px; }
    .inline-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .node-card {
      margin-bottom: 6px; padding: 8px; background: #1f2937;
      border-radius: 6px; font-size: 0.78rem; line-height: 1.5;
    }
    .node-name { font-weight: bold; color: #93c5fd; }
    .node-id   { color: #9ca3af; }
    #messages { flex: 1; overflow-y: auto; padding: 12px; }
    .message {
      margin-bottom: 8px; padding: 9px; border-radius: 6px;
      background: #20293a; font-size: 0.82rem; line-height: 1.4;
      overflow-wrap: anywhere;
    }
    .message.outbound { background: #12363a; }
    .message-meta { color: #a7b2c4; font-size: 0.72rem; margin-bottom: 4px; }
    .message-name { color: #93c5fd; font-weight: 700; }
    .empty { color: #7d8796; font-size: 0.78rem; }
    #no-pos-list { max-height: 170px; overflow-y: auto; }
    #send-status { min-height: 17px; color: #5eead4; font-size: 0.76rem; margin-top: 8px; }
    #status {
      position: absolute; bottom: 10px; left: 10px; z-index: 1000;
      background: rgba(0,0,0,0.65); color: #34d399;
      padding: 4px 10px; border-radius: 12px; font-size: 0.75rem; pointer-events: none;
    }
    @media (max-width: 760px) {
      body { grid-template-columns: 1fr; grid-template-rows: 48vh 52vh; }
      #sidebar { border-left: 0; border-top: 1px solid #2f3a4b; }
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="sidebar">
    <form id="send-form" class="panel">
      <h2>Send To Mesh</h2>
      <div class="field">
        <label for="message">Message</label>
        <textarea id="message" maxlength="225" required></textarea>
      </div>
      <div class="inline-grid">
        <div class="field">
          <label for="destination-type">Destination</label>
          <select id="destination-type">
            <option value="primary">Primary channel</option>
            <option value="node">Node number</option>
          </select>
        </div>
        <div class="field">
          <label for="destination-node">Node number</label>
          <input id="destination-node" inputmode="numeric" pattern="[0-9]*" disabled>
        </div>
      </div>
      <button id="send-button" type="submit">Send</button>
      <div id="send-status"></div>
    </form>
    <div id="messages"></div>
    <div class="panel">
      <h2>No Position Data</h2>
      <div id="no-pos-list"></div>
    </div>
  </div>
  <div id="status">Loading...</div>
  <script>
    const map = L.map('map').setView([20, 0], 2);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
    }).addTo(map);

    let markers = [];

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[c]));
    }

    function updateMap() {
      fetch('/api/nodes')
        .then(r => r.json())
        .then(nodes => {
          markers.forEach(m => map.removeLayer(m));
          markers = [];

          const noPos = [];
          nodes.forEach(node => {
            if (node.lat !== null && node.lon !== null) {
              const alt = node.alt !== null ? node.alt + 'm' : 'N/A';
              const popup = `<b>${escapeHtml(node.longname)}</b> (<code>${escapeHtml(node.id)}</code>)<br>
                <b>Num:</b> ${escapeHtml(node.num)}<br>
                <b>SNR:</b> ${escapeHtml(node.snr)}<br>
                <b>Hops:</b> ${escapeHtml(node.hopsaway)}<br>
                <b>Alt:</b> ${escapeHtml(alt)}<br>
                <b>Last heard:</b> ${escapeHtml(node.lastheardutc)}`;
              const m = L.marker([node.lat, node.lon]).addTo(map).bindPopup(popup);
              markers.push(m);
            } else {
              noPos.push(node);
            }
          });

          const list = document.getElementById('no-pos-list');
          if (noPos.length === 0) {
            list.innerHTML = '<div style="color:#6b7280;font-size:0.78rem">All nodes have position data.</div>';
          } else {
            list.innerHTML = noPos.map(n => `
              <div class="node-card">
                <span class="node-name">${escapeHtml(n.longname)}</span><br>
                <span class="node-id">${escapeHtml(n.id)}</span><br>
                SNR: ${escapeHtml(n.snr)} &nbsp; Hops: ${escapeHtml(n.hopsaway)}<br>
                Last: ${escapeHtml(n.lastheardutc)}
              </div>`).join('');
          }

          document.getElementById('status').textContent =
            'Updated: ' + new Date().toLocaleTimeString();
        })
        .catch(() => {
          document.getElementById('status').textContent = 'reconnecting…';
        });
    }

    function updateMessages() {
      fetch('/api/messages')
        .then(r => r.json())
        .then(messages => {
          const list = document.getElementById('messages');
          if (messages.length === 0) {
            list.innerHTML = '<div class="empty">No mesh messages yet.</div>';
            return;
          }
          list.innerHTML = messages.map(m => {
            const outbound = m.direction === 'outbound';
            const name = outbound ? 'Web' : (m.longname || m.node_id || 'Mesh');
            const target = m.to_id ? ` to ${escapeHtml(m.to_id)}` : '';
            const channel = m.channel_index !== null && m.channel_index !== undefined ? ` ch ${escapeHtml(m.channel_index)}` : '';
            return `<div class="message ${outbound ? 'outbound' : ''}">
              <div class="message-meta">
                <span class="message-name">${escapeHtml(name)}</span>${target}${channel}
                <br>${escapeHtml(m.timestamputc || '')}
              </div>
              <div>${escapeHtml(m.text)}</div>
            </div>`;
          }).join('');
        });
    }

    const destinationType = document.getElementById('destination-type');
    const destinationNode = document.getElementById('destination-node');
    destinationType.addEventListener('change', () => {
      const isNode = destinationType.value === 'node';
      destinationNode.disabled = !isNode;
      if (!isNode) destinationNode.value = '';
    });

    document.getElementById('send-form').addEventListener('submit', event => {
      event.preventDefault();
      const status = document.getElementById('send-status');
      const button = document.getElementById('send-button');
      const payload = { message: document.getElementById('message').value };
      if (destinationType.value === 'node') payload.destination = destinationNode.value;
      status.textContent = 'Sending...';
      button.disabled = true;
      fetch('/api/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
        .then(async r => {
          const data = await r.json();
          if (!r.ok) throw new Error(data.error || 'Could not send message');
          document.getElementById('message').value = '';
          status.textContent = 'Queued for mesh.';
          updateMessages();
        })
        .catch(error => {
          status.textContent = error.message;
        })
        .finally(() => {
          button.disabled = false;
        });
    });

    updateMap();
    updateMessages();
    setInterval(updateMap, 5000);
    setInterval(updateMessages, 3000);
  </script>
</body>
</html>"""


def create_map_app():
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    app = Flask(__name__)

    @app.route('/api/nodes')
    def api_nodes():
        with nodes_lock:
            snapshot = list(all_nodes)
        return jsonify(snapshot)

    @app.route('/api/messages', methods=['GET'])
    def api_messages():
        limit = request.args.get('limit', default=MAX_WEB_MESSAGES, type=int)
        limit = max(1, min(limit, MAX_WEB_MESSAGES))
        with messages_lock:
            snapshot = list(mesh_messages[-limit:])
        return jsonify(snapshot)

    @app.route('/api/messages', methods=['POST'])
    def api_send_message():
        data = request.get_json(silent=True) or {}
        text = str(data.get('message', '')).strip()
        if not text:
            return jsonify({'error': 'Message is required.'}), 400
        if len(text) > 225:
            return jsonify({'error': 'Message must be 225 characters or fewer.'}), 400

        destination = data.get('destination')
        if destination in ('', None):
            destination = None
        elif str(destination).isdigit():
            destination = int(destination)
        else:
            return jsonify({'error': 'Destination node number must be numeric.'}), 400

        discordtomesh.put(format_outbound_mesh_message(text, destination))
        message = add_mesh_message({
            'direction': 'outbound',
            'source': 'web',
            'longname': 'Web',
            'node_id': 'web',
            'to_id': str(destination) if destination is not None else '^all',
            'channel_index': primary_mesh_channel_index if destination is None else None,
            'text': text,
        })
        return jsonify({'status': 'queued', 'message': message}), 202

    @app.route('/')
    def index():
        return MAP_HTML

    return app


def start_map_server():
    app = create_map_app()
    thread = threading.Thread(
        target=app.run,
        kwargs={'host': '0.0.0.0', 'port': 8765, 'use_reloader': False},
        daemon=True,
    )
    thread.start()
    print('Web bridge started at http://0.0.0.0:8765')


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
                "$nodeinfo <id> returns full details for a node by hex ID (e.g. !abc123) or node number\n"\
                "Set DISCORD_SECONDARY_CHANNEL_ID to forward non-primary mesh messages to another Discord channel\n"\
                "Web bridge available at http://<your-host>:8765"
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

        if message.content.startswith('$nodeinfo'):
            query = message.content[9:].strip()
            if not query:
                await message.channel.send('Usage: `$nodeinfo <hex id or node number>`')
                return
            with nodes_lock:
                snapshot = list(all_nodes)
            node = find_node(snapshot, query)
            if node is None:
                await message.channel.send(f'Node `{query}` not found.')
                return
            if node['lat'] is not None:
                pos = f"{node['lat']:.5f}, {node['lon']:.5f}"
                if node['alt'] is not None:
                    pos += f" (alt: {node['alt']}m)"
            else:
                pos = 'Not available'
            embed = discord.Embed(
                title=f"📡 {node['longname']} ({node['id']})",
                color=0x00aaff,
            )
            embed.add_field(name='Num', value=node['num'], inline=True)
            embed.add_field(name='Hops away', value=node['hopsaway'], inline=True)
            embed.add_field(name='SNR', value=node['snr'], inline=True)
            embed.add_field(name='Last heard (UTC)', value=node['lastheardutc'], inline=False)
            embed.add_field(name='Position', value=pos, inline=False)
            await message.channel.send(embed=embed)


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
                            timestr = datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
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
                cutoff = time.time() - (15 * 60)
                with nodes_lock:
                    snapshot = list(all_nodes)
                recent = [n for n in snapshot if n['ts'] > cutoff]
                if not recent:
                    await primary_channel.send(embed=discord.Embed(
                        title="🟢 Active Nodes",
                        description="No active nodes found in the last 15 minutes.",
                        color=0x00ff99,
                    ))
                else:
                    for offset in range(0, len(recent), 25):
                        embed = discord.Embed(title="🟢 Active Nodes", color=0x00ff99)
                        for node in recent[offset:offset + 25]:
                            pos_line = ''
                            if node['lat'] is not None:
                                pos_line = f"\n📍 `{node['lat']:.5f}, {node['lon']:.5f}`"
                                if node['alt'] is not None:
                                    pos_line += f" (alt: `{node['alt']}m`)"
                            embed.add_field(
                                name=f"📡 {node['longname']} (`{node['id']}`)",
                                value=f"SNR: `{node['snr']}` | Last heard: `{node['lastheardutc']}`{pos_line}",
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
