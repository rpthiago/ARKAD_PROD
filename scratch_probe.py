import urllib.request
hosts = [
    'api-ng.betfair.com',
    'identitysso.betfair.com',
    'identitysso-cert.betfair.com',
    'stream-api.betfair.com',
    'ero.betfair.com',
    'ips.betfair.com',
    'mobile.betfair.com',
    'apimg.betfair.com',
    'developer.betfair.com',
]
for h in hosts:
    try:
        req = urllib.request.Request('https://' + h + '/', headers={'User-Agent': 'Mozilla/5.0'})
        res = urllib.request.urlopen(req, timeout=5)
        print(h, '->', res.status)
    except urllib.error.HTTPError as e:
        print(h, '-> HTTP', e.code)
    except Exception as e:
        print(h, '-> ERR:', type(e).__name__)
