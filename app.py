from chalice import Chalice
from chalicelib.s3_manager import S3ManagerCore
import logging
import json
import time

app = Chalice(app_name='imager')
app.debug = True
app.log.setLevel(logging.DEBUG)


@app.on_sqs_message(queue='imager-queue', batch_size=1)
def build(event):
    #app.log.info("Event: %s", event.to_dict())
    event = event.to_dict()
    status_code = 200
    records = event.get('Records', None)
    try:
        if records is not None and records:
            for rec in records:
                print(rec)
                try:
                    body = json.loads(rec['body'])
                    domain_id = body.get('domain_id')
                    filename = body.get('filename')
                    url = body.get('url')
                    build_core(domain_id, filename, url)
                except Exception as e:
                    app.log.error(e)
    except Exception as e:
        status_code = 400

    response = {
        "statusCode": status_code,
        "body": json.dumps(event)
    }
    return response

def build_core(domain_id, filename, url):
    s3FileManager = S3ManagerCore(type="prd")
    start_time = time.time()
    cprs = 90
    from chalicelib.imager import Imager
    imager = Imager(url=url)
    imager.compress(cprs)
    group = 'compress-{}'.format(cprs)
    file_key = s3FileManager.get_path(
        shop=domain_id, group=group, filename=filename)
    print(file_key)
    if imager.current_bytesio is not None:
        bytesio = imager.current_bytesio.getvalue()
        s3FileManager.upload_bytes(
            bytesio=bytesio, file_key=file_key, filename=filename)
        print(s3FileManager.get_path(shop=domain_id, group=group,
                                     filename=filename, full_url_flag=True))
    print(time.time() - start_time)


def test(event=None, context=None):
    build_core(1, 'test.jpg',
            "url")

if __name__ == '__main__':
    test()
