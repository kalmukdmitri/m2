import requests
# https://yandex.ru/dev/id/doc/ru/codes/screen-code

# Получаем код 

# https://oauth.yandex.ru/client/9527c706e55f4be980755bc5063cb1f4

client_id = ''

client_secret = ''

redirect_uri= 'https://oauth.yandex.ru/verification_code'

code_url = f'https://oauth.yandex.ru/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}'
# Идём по урлу для получения кода
print(code_url) 

code = ''
token_req_test = f'https://oauth.yandex.ru/token'
body = {
    'grant_type':'authorization_code',
    'code':code,
    'client_id':client_id,
    'client_secret':client_secret
}
g = requests.post(token_req_test,data=body )
g.text