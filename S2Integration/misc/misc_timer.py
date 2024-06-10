import time

# TODO: Not understood
def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        elapsed_time = end_time - start_time

        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        print(f"Execution time for {func.__name__}: {int(hours)}h, {int(minutes)}m, {seconds:.2f}s")

        return result
    return wrapper


def format_time(start_time, end_time):
    elapsed_time = end_time - start_time

    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{int(hours)}h, {int(minutes)}m, {seconds:.2f}s"
