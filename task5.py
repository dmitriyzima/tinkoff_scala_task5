from enum import Enum
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor

timeout_seconds = timedelta(seconds=15).total_seconds()
loop = asyncio.get_event_loop()
class Response(Enum):
  Success = 1
  RetryAfter = 2
  Failure = 3

class ApplicationStatusResponse(Enum):
  Success = 1
  Failure = 2

@dataclass
class ApplicationResponse:
  application_id: str
  status: ApplicationStatusResponse
  description: str
  last_request_time: datetime
  retriesCount: Optional[int]

async def get_application_status1(identifier: str) -> Response:
  # Метод возвращает случайный статус заявки от сервиса 1
  import random
  return random.choice(list(Response))

async def get_application_status2(identifier: str) -> Response:
  # Метод возвращает случайный статус заявки от сервиса 2
  import random
  return random.choice(list(Response))

async def perform_operation(identifier: str) -> ApplicationResponse:
  start_time = datetime.now()
  retries_count = 0
  # Создаем ThreadPoolExecutor для запуска блокирующих функций в отдельных потоках и список задач для запуска
  executor = ThreadPoolExecutor(max_workers=2)
  tasks = [loop.run_in_executor(executor, get_application_status1, identifier),
           loop.run_in_executor(executor, get_application_status2, identifier)]
  
  done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
  for task in pending:
    task.cancel()
    
  # Получим результаты работы
  results = [task.result() for task in done]
  
  # если все задачи завершились успешно
  if len(results) == 2 and all(result == Response.Success for result in results):
    # успешный ответ
    return ApplicationResponse(
      application_id=identifier,
      status=ApplicationStatusResponse.Success,
      description="Заявка успешно обработана обоими сервисами",
      last_request_time=datetime.now(),
      retriesCount=None
    )
  # Проверяем, был ли RetryAfter
  elif Response.RetryAfter in results:
    # Увеличиваем счетчик
    retries_count += 1
    # Проверяем, что не превышен таймаут
    if (datetime.now() - start_time).total_seconds() < timeout_seconds:
      await asyncio.sleep(1)
      # вызываем perform_operation с тем же идентификатором (рекурсия)
      return await perform_operation(identifier)
    else:
      # Возвращаем неудачный ответ с превышением таймаута
      return ApplicationResponse(
        application_id=identifier,
        status=ApplicationStatusResponse.Failure,
        description="Заявка не обработана. Превышение таймаута",
        last_request_time=datetime.now(),
        retriesCount=retries_count
      )
  else:
    # Возвращаем неудачный ответ с ошибкой одного или обоих сервисов
    return ApplicationResponse(
      application_id=identifier,
      status=ApplicationStatusResponse.Failure,
      description="Один или оба сервиса вернули ошибку",
      last_request_time=datetime.now(),
      retriesCount=None
    )
