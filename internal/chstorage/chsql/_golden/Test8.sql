SELECT body FROM logs PREWHERE hasToken(body, 'Error')