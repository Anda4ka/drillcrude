# CRUDE Driller v6.5

Автоматический бот для майнинга $CRUDE токенов на Base через [drillcrude.com](https://www.drillcrude.com).

## Что делает

Async loop-ы:

- **Drilling** — основной цикл: auth -> выбор лучшего сайта -> получение challenge -> **детерминистическое решение** -> submit -> inline receipt -> cooldown (30s) -> repeat
- **Claiming** — каждые 30 мин проверяет завершённые эпохи и клеймит награды
- **Monitoring** — каждые 5 мин логирует статистику (solve rate, credits, gushers, per-site stats)

## Ключевая фича: Deterministic Solver

Бот **не использует LLM** для решения challenges. Вместо этого:

1. Парсит документ регулярками -> извлекает данные каждой компании (employees, founded, revenue, margin, **город, сектор**)
2. Классифицирует вопрос -> определяет поле и направление (highest revenue, fewest employees, etc.)
3. **Применяет фильтры** из вопроса: город ("headquartered in Houston"), сектор ("upstream exploration"), год ("founded before 1990")
4. Выбирает компанию алгоритмически -> `max()`/`min()` по нужному полю среди отфильтрованных
5. Вычисляет артифакт в Python -> `mod`, `letter_positions`, `first_n_reversed`, etc.
6. При реджекте — пробует альтернативные варианты (tied компании) **Alt-retry теперь работает!**

**Результат:** ~74% hit rate, 0 затрат на LLM API.

LLM используется только как fallback для неизвестных типов вопросов (GLM-5 бесплатно).

## Требования

- Python 3.10+
- [Bankr](https://bankr.bot) API key с write-доступом
- Застейканные $CRUDE токены (минимум 25M для Wildcat tier)
- ETH на Base для газа (~$0.001 за receipt транзакцию)
- *(Опционально)* [OpenRouter](https://openrouter.ai) или ZAI API key — только для LLM fallback

## Установка

```bash
git clone https://github.com/Anda4ka/drillcrude.git
cd drillcrude

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/Mac

pip install aiohttp openai zai-sdk
```

### VPS (Ubuntu 22.04)

```bash
bash setup_vps.sh
# или вручную:
apt install -y python3 python3-venv
mkdir -p ~/driller && cd ~/driller
python3 -m venv venv && source venv/bin/activate
pip install aiohttp openai zai-sdk
```

## Настройка

Создай `.env`:

| Переменная | Обязательно | Описание |
|---|---|---|
| `BANKR_API_KEY` | ✅ | API ключ от Bankr |
| `DRILLER_ADDRESS` | ✅ | Твой Base wallet address |
| `COORDINATOR_URL` | | URL координатора (default: production) |
| `DRILLER_DEBUG` | | `true` для подробных логов |
| `DRILLER_QUIET` | | `true` для минимального вывода (только accepts/errors) |
| `LLM_BACKEND` | | `zai` (бесплатно) или `openrouter` |
| `LLM_MODEL` | | Модель для LLM fallback (default: `glm-5`) |
| `ZAI_API_KEY` | | Нужен только если LLM_BACKEND=zai |
| `OPENROUTER_API_KEY` | | Нужен только если LLM_BACKEND=openrouter |

## Запуск

```bash
# Windows PowerShell
python crude_driller.py

# С debug-логами
$env:DRILLER_DEBUG="true"; python crude_driller.py

# Тихий режим
$env:DRILLER_QUIET="true"; python crude_driller.py

# Linux
python3 crude_driller.py

# Linux (фон через screen)
screen -dmS driller bash -c 'cd ~/driller && source venv/bin/activate && python3 crude_driller.py'
# Подключиться: screen -r driller
# Отключиться: Ctrl+A, D
```

Остановка — `Ctrl+C` (чистое завершение с сохранением state).

## Архитектура solver

```
Challenge -> deterministic_pass1() -> compute_artifact_locally() -> submit
                   | (fail)                                          | (rejected)
              LLM Pass 1 -> local compute -> submit              try alternates
                               | (fail)                           (NOW WORKS!)
                          LLM Pass 2 -> submit
```

### Поддерживаемые constraint-ы

| Тип | Пример | Hit rate |
|---|---|---|
| margin mod/mul/sub/add | `margin * 3` -> `54` | **100%** |
| founding_year mod N | `founding_year mod 19` -> `0` | **96.6%** |
| employees mod N | `employees mod 7` -> `1` | **91.2%** |
| letter positions | `positions 2,5,8` -> chars | 66.7% |
| first N reversed | `Summit` (4) -> `mmuS` | 65.5% |
| first letters | `Blue Mesa Logistics` -> `BML` | 63.0% |
| revenue mod N | `revenue mod 13` -> `5` | 17.6% |
| every Nth letter | `every 3rd` -> extract | 0% |

### Фильтры вопросов

| Фильтр | Пример |
|---|---|
| Город | "headquartered in Houston" |
| Сектор | "oilfield services company" |
| Год | "founded before 1990" |
| Revenue/employee | "highest revenue-per-employee ratio" |

## Trace формат (v6.5)

Координатор строго валидирует trace. Формат:

```json
[
  {"type": "locate_entity", "entity": "Company Name", "paragraph": 3},
  {"type": "extract_value", "entity": "Company Name", "field": "revenue", "value": "$8.1B", "paragraph": 3},
  {"type": "apply_constraint", "description": "8100 mod 17", "operation": "8100 % 17", "result": "14"}
]
```

**Правила:**
- Минимум 3 шага
- `paragraph` — 1-indexed (split на `\n\n`)
- Валидные поля: `revenue`, `employees`, `founded`, `margin`
- Revenue — raw значение из документа (`"$8.7B"`, не `"8700M"`)

## Drill cooldown (v6.5)

- **30 сек** серверный cooldown после каждого accept
- После reject — без паузы, сразу следующий drill
- Alt-retry тоже без паузы
- Receipt постится inline (blocking) — обязателен для зачёта credits

## Выбор сайтов

Приоритет: bonanza > rich > standard, затем shallow > medium (74% vs 58% hit rate).

| Тип | Множитель | Описание |
|---|---|---|
| standard | 1x | Базовые кредиты |
| rich | 4x | Повышенная награда |
| bonanza | 5x | Максимальная награда |

Плюс: gusher (3x), mega-gusher (10x), WTI oracle множитель (0.5x-1.5x).

## Staking tiers

| Tier | Стейк | Credits/solve | Доступ |
|---|---|---|---|
| Wildcat | 25M+ $CRUDE | 1 | Shallow |
| Platform | 50M+ $CRUDE | 2 | Shallow + Medium |
| Deepwater | 100M+ $CRUDE | 3 | Все сайты |

## Файлы

| Файл | Описание |
|---|---|
| `crude_driller.py` | Основной скрипт (~1800 строк) |
| `setup_vps.sh` | Скрипт установки на VPS (Ubuntu 22.04) |
| `claim_now.py` | Ручной клейм наград |
| `.env` | Конфигурация (не коммитить!) |
| `crude_driller.log` | Основной лог |
| `crude_debug.log` | Debug-лог |
| `crude_driller_state.json` | Персистентное состояние |

Логи авто-ротируются при >10 МБ (остаётся последние 5 МБ).

## Troubleshooting

**"Drill cooldown active — wait Ns"** — серверный cooldown 30 сек после accept. Бот ждёт автоматически.

**"Unsupported extract_value field"** — координатор изменил формат trace. Проверь что trace использует только: `revenue`, `employees`, `founded`, `margin`.

**"Trace must reference the constraint company"** — бот выбрал не ту компанию. Alt-retry попробует альтернативы.

**"Miner is not eligible to drill"** — проверь стейк. Unstake pending блокирует drilling.

**Auth 502 errors** — координатор временно недоступен. Бот retry-ит с backoff.

## Версии

| Версия | Что нового |
|---|---|
| v6.0 | Deterministic solver — без LLM для 95% challenges |
| v6.1 | Alt-retry при реджекте + фиксы пробелов |
| v6.2 | Throttled receipts, stale drill fix, авто-ротация логов |
| v6.3 | Question filters (город/сектор/год), Platform tier, quiet mode |
| v6.4 | I/O оптимизации, shallow preference, VPS support |
| v6.5 | **API update**: новый trace формат, inline receipts, 30s cooldown, alt-retry работает |

### Changelog v6.5

**API совместимость (breaking changes координатора):**
- Trace валидация: `locate_entity` → `extract_value` → `apply_constraint` (min 3 шага)
- Поля: `company_name` → убрано, `operating_margin` → `margin`
- Revenue в trace: raw значение (`"$8.7B"`) вместо parsed (`"8700M"`)
- Submit response: `crudeLotId` вместо прямого `transaction`
- `siteId` обязателен в submit payload

**Alt-retry теперь работает:**
- Координатор больше не закрывает challenge после первого rejection
- При rejected → бот пробует альтернативные tied-компании
- Улучшает effective hit rate

**Inline receipts:**
- Receipt постится сразу после accept (blocking)
- Receipt ОБЯЗАТЕЛЕН — credits не засчитываются без on-chain receipt
- `receipt-calldata` endpoint для повторного получения transaction

**Drill cooldown:**
- 30 сек серверный cooldown после каждого accept
- Парсинг точного времени из 429 response
- После reject — без паузы
