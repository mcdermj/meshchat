from flask import Flask, Response, request, g, send_from_directory, redirect
from flask_apscheduler import APScheduler
from meshchat import MeshChat, InvalidExtensionError
import hashlib
import time
import json


class Config(object):
    SCHEDULER_API_ENABLED = True


app = Flask(__name__)
app.config.from_object(Config())

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()


def get_chat():
    chat = getattr(g, '_chat', None)
    if chat is None:
        chat = g._chat = MeshChat()
    return chat


with app.app_context():
    print('Refreshing Nodes...')
    with scheduler.app.app_context():
        get_chat().refresh_node_list()
    print('Done.')


@scheduler.task('interval', id='refresh_messages', seconds=10, misfire_grace_time=100)
def refresh_tables():
    print('Refreshing Messages...')
    with scheduler.app.app_context():
        get_chat().refresh_messages()
    print('Done.')


@scheduler.task('interval', id='refresh_nodes', seconds=600, misfire_grace_time=100)
def refresh_nodes():
    print('Refreshing Nodes...')
    with scheduler.app.app_context():
        get_chat().refresh_node_list()
    print('Done.')


@scheduler.task('interval', id='refresh_users', seconds=60, misfire_grace_time=100)
def refresh_users():
    print('Refreshing Users...')
    with scheduler.app.app_context():
        get_chat().refresh_users()
    print('Done.')


@scheduler.task('interval', id='refresh_files', seconds=120, misfire_grace_time=100)
def refresh_files():
    print('Refreshing Files...')
    with scheduler.app.app_context():
        get_chat().refresh_files()
    print('Done.')


@app.route('/cgi-bin/meshchat', methods=['GET', 'POST'])
def cgi():
    if request.args.get('action') == 'upload_file':
        return do_upload_file(request.args)

    args = request.args if request.method == 'GET' else request.form
    action = args.get('action')

    try:
        return globals()[f'do_{action}'](args)
    except KeyError:
        return (f'Invalid Action: {action}', 404)


@app.route('/meshchat/<path:filename>')
def static_content(filename):
    return send_from_directory('static_content', filename)


@app.route('/meshchat/')
def index_page():
    return send_from_directory('static_content', 'index.html')


@app.route('/meshchat')
def redirect_from_root():
    return redirect("/meshmap/", code=302)


def do_messages(args):
    get_chat().update_local_user(args.get('call_sign'),
                                 args.get('id'), max(int(args.get('epoch', 0)), int(time.time())))
    return Response(response=get_chat().get_json_messages(), status=200, mimetype='application/json')


def do_config(args):
    return {
        'version': get_chat().version,
        'node': get_chat().node,
        'zone': get_chat().zone
    }


def do_sync_status(args):
    return Response(response=get_chat().get_json_nodes(), status=200, mimetype='application/json')


def do_messages_raw(args):
    return send_checksummed_text(get_chat().get_raw_messages())


def do_messages_md5(args):
    return Response(response=hashlib.md5(get_chat().get_raw_messages().encode('utf-8')).hexdigest(), status=200,
                    mimetype='text/plain')


def do_messages_download(args):
    response = send_checksummed_text(get_chat().get_raw_messages())
    response.headers['Content-Disposition'] = 'attachment; filename=messages.txt;'
    return response


def do_users_raw(args):
    return send_checksummed_text(get_chat().get_raw_users())


def do_users(args):
    return Response(response=get_chat().get_json_users(), status=200, mimetype='application/json')


def do_local_files_raw(args):
    return send_checksummed_text(get_chat().get_raw_files(local_only=True))


def do_files(args):
    return({
        "stats": get_chat().get_file_stats(),
        "files": get_chat().get_dict_files()
    })


def do_messages_version(args):
    return Response(response=str(get_chat().get_message_version()), status=200, mimetype='text/plain')


def do_messages_version_ui(args):
    call = args.get('call_sign')
    if call is not None:
        get_chat().update_user_epoch(call, max(int(args.get('epoch', 0)), int(time.time())))

    return {"messages_version": get_chat().get_message_version()}


def do_hosts(args):
    return Response(response=json.dumps([]), status=200, mimetype='application/json')


def do_hosts_raw(args):
    return Response(response=None, status=200, mimetype='application/json')


def do_send_message(args):
    if 'message' not in args:
        return ("Required parameter 'message' missing", 400)
    if 'call_sign' not in args:
        return ("Required parameter 'call_sign' missing", 400)

    get_chat().create_message(args.get('call_sign'), args.get('message'), args.get('channel'),
                              max(int(args.get('epoch', 0)), int(time.time())))

    return {"status": 200, "response": "OK"}


def do_upload_file(args):
    if 'uploadfile' not in request.files:
        return ("No file uploaded", 400)

    file = request.files['uploadfile']

    try:
        get_chat().store_file(file)
    except InvalidExtensionError as e:
        return ({"status": 500, "response": str(e)}, 500)

    return {"status": 200, "response": "OK"}


def do_delete_file(args):
    get_chat().delete_file(args.get('file'))
    return {'status': 200, 'response': 'OK'}


def do_file_download(args):
    filename = get_chat().get_path_for_file(args.get('file'))
    return send_from_directory(filename.parent, filename.name)


# TODO: Implement
def do_meshchat_nodes(args):
    response = '\n'.join(['\t'.join([node[0], node[1]]) for node in get_chat().get_node_list(alive=False)])
    return Response(response=response, status=200, mimetype='text/plain')


# TODO: Implement
def do_action_log(args):
    return ("Not Implemented", 501)


def send_checksummed_text(response_text):
    response = Response(response=response_text, status=200, mimetype='text/plain')
    #  TODO: This should be set sanely?  Is this really ASCII?
    response.headers['Content-MD5'] = hashlib.md5(response_text.encode('utf-8')).hexdigest()
    return response



# def test_job():
#     print('I am working...')
#
#
# scheduler = BackgroundScheduler()
# job = scheduler.add_job(test_job, 'interval', minutes=1)
# scheduler.start()