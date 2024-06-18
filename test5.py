responses_condition = {
    'http1': {200: 23, 404: 12},
    'http2': {429: 3, 200: 123}
}

# for url in responses_condition.values():
#     print(sum(url.values()))

print(sum(sum(url.values()) for url in responses_condition.values()))