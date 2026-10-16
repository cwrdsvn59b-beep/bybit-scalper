> Этот отчёт описывает коммит 352389b. Дополнительные найденные ошибки и их исправления: [AUDIT_REVIEW.md](AUDIT_REVIEW.md).

# Проверка 16 октября 2026, ветка spec-hardening

Исходный коммит задания: `9a9453377b88895ac21e5ee4d7b58c4ba37debba`.
Архива `bybit-scalper-audited.zip` в окружении не было. Его содержимое уже находится в коммите `afed6e80cf13ac48e8a7331593d10808665a71a6` на `origin/master` и `origin/audit-fixes` (проверено `git fetch`, не предположено). Этот проход не переписывает ту работу и не двигает `master`.

Новые изменения лежат поверх `afed6e8`: выделенный капитал, журнал funding, чтение маржи без смены настроек счёта, раздельные очереди, сбор и replay.

## Что сделано в этом проходе

- `allocated_capital` ограничивает риск и дневной порог. Весь UNIFIED equity бюджетом не назначается.
- Дневной порог — 1% выделенного капитала. В него входят открытый результат, комиссии и funding текущих суток UTC. Переводы средств отдельной строкой и в торговый PnL не попадают.
- Переход UTC-суток фиксирует базу открытого результата так, чтобы funding новых суток не спрятался в базе прошлого дня.
- `read_account` читает equity, available и `marginMode`. Подтверждённым считается только `REGULAR_MARGIN` вместе с one-way `tradeMode=0`. Isolated и portfolio новые входы не открывают: account-wide поля для isolated неприменимы. `set_leverage`, `switch_margin_mode`, `switch_position_mode` не вызываются.
- Перед входом testnet/live заново читает счёт. Резерв должен помещаться и в доступную маржу, и в `allocated_capital`. Если за время проверки стакан устарел, вход пропускается.
- Funding берётся только из поля `funding` записи `SETTLEMENT` в transaction-log. `cashFlow` для этого не используется. Интервал загружается целиком и заменяется одной транзакцией; ошибка страницы оставляет прежний снимок и снимает подтверждение. Совпадение execId/tradeId между API не считается протоколом.
- Пока журнал не подтверждён, вход, способный пересечь funding, запрещён. В paper/testnet/live неизвестное время funding тоже запрещает вход. Offline биржевых часов не имеет; это написано в отчёте, а не выдано за подтверждение биржи.
- Live отказывается от `data/state.db`. Отдельные файлы: paper `data/state.db`, testnet `data/state-testnet.db`, live `data/state-live.db`.
- Публичная очередь по-прежнему сбрасывает лишние котировки. Приватные события идут раньше и не вытесняются котировками. Переполнение приватной очереди блокирует входы.
- Сделка ленты раньше открытия заявки не исполняет paper-лимит. Рост позиции повторно ставит Full stop. Поздний входной fill при незавершённом выходе не шлёт второе закрытие.
- `collect` при сетевой ошибке выходит с текстом и не создаёт фиктивную ленту. `replay` без файлов прямо говорит, что статистика не выдумана.
- Блокировка процесса — `flock` одного файла. Второй процесс с другой базой или на другой машине не останавливается. Это один экземпляр на аккаунт, не кластер.

Сохранены прежние правила `afed6e8`: cancel не является конечным статусом, выход запоминает `orderLinkId` до сети, учёт исполнения атомарен, стакан не требует `u = previous + 1`, 5m строится из пяти разных минут, paper всегда слушает ленту.

## Команды, которые выполнены здесь

Python 3.10.21, pybit 5.17.0. Реальных ключей нет, ордера на биржу не отправлялись.

```text
python -m pytest --tb=no
70 passed, 1 skipped in 0.93s
```

Пропущен `test_testnet_public_time_is_opt_in`: нет `BYBIT_INTEGRATION=1`.

```text
PYTHONPATH=src python -m compileall -q src
PYTHONPATH=src python -m bybit_scalper offline --config config/default.yaml
PYTHONPATH=src python -m bybit_scalper replay --config config/default.yaml
```

Offline: 420 синтетических минутных свечей. На независимом хвосте 1 сигнал, 1 заявка, 0 исполнений, PnL 0. Контур на нарисованной ленте: 1 сделка, чистый PnL 16.2437479824690. Это не рыночный результат. Replay без файлов напечатал: «Лента не собрана. Статистика не выдумана.»

## Сеть

Публичный read-only запрос времени с этой машины, 16 октября 2026 около 12:36 UTC:

- `GET https://api.bybit.com/v5/market/time` — HTTP 403, CloudFront «block access from your country».
- pybit: `FailedRequestError` 403 «your ip is from the usa».
- `GET https://api-testnet.bybit.com/v5/market/time` — тот же HTTP 403 и та же ошибка pybit.

Paper, WebSocket, авторизованный Testnet и live с этой машины не запускались. `collect` здесь тоже не может скачать ленту: тот же хост.

## Что мешает реальной торговле

1. Нет интеграционного прогона Testnet: частичные исполнения, стоп, перезапуск, обрыв приватного сокета и неизвестный ответ закрытия на серверах Bybit не подтверждались.
2. Нет оценки ожидания на независимой рыночной истории. 420 синтетических свечей и PnL контура 16.24 доказательством не являются.
3. Очередь paper — заданный объём перед лимитом, не модель скрытой ликвидности и места в очереди.
4. Журнал funding написан и покрыт поддельными ответами API. Живой `transaction-log` и пагинация Bybit отсюда не читались. Пока синхронизация не подтверждена, бот и не должен держать позицию через funding.
5. Чтение маржи не проверялось на боевом или testnet-счёте. Неверный `tradeMode` или portfolio блокируют вход в коде, но конкретный режим аккаунта пользователя неизвестен.
6. `flock` защищает один локальный файл. Две базы или два сервера на одном ключе не запрещены. Чужие входные заявки ловятся только в момент старта.
7. Windows, обрыв диска и реальный режим маржи не проверялись. `LIVE_CONFIRM=YES` не означает готовности.

## Протоколы, на которые опирается код

- https://bybit-exchange.github.io/docs/v5/order/create-order
- https://bybit-exchange.github.io/docs/v5/order/cancel-order
- https://bybit-exchange.github.io/docs/v5/order/execution
- https://bybit-exchange.github.io/docs/v5/position/trading-stop
- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- https://bybit-exchange.github.io/docs/v5/account/transaction-log
- https://bybit-exchange.github.io/docs/v5/account/account-info
- https://bybit-exchange.github.io/docs/v5/market/recent-trade
