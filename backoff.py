from functools import wraps
import logging
import time


def backoff(
    start_sleep_time=0.1,
    factor=2,
    border_sleep_time=10,
    max_iter=10,
    exception_type=Exception,
):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            current_sleep_time = start_sleep_time
            iter_count = 0
            while iter_count < max_iter:
                try:
                    iter_count += 1
                    return func(*args, **kwargs)
                except exception_type as e:
                    logging.error(f"Подключиться не удалось: {e}.")
                    logging.info("Повторное подключение.")
                    time.sleep(current_sleep_time)
                    current_sleep_time *= factor
                    if current_sleep_time > border_sleep_time:
                        current_sleep_time = border_sleep_time

        return inner

    return func_wrapper
