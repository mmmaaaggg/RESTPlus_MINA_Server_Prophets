# tasks.py
from app.app import app
from celery_task import make_celery
from celery_task.data_analysis import update_compare_pl_all, update_pl_all
import logging
logger = logging.getLogger()
celery = make_celery(app)


@celery.task
def update_compare_pl_all_task():
    update_compare_pl_all()


@celery.task
def update_pl_all_task():
    update_pl_all()


@celery.task
def chain_task():
    """
    update_pl_all -> update_compare_pl_all
    更新 投资组合净值计算，然后更新比较列表
    :return:
    """
    # 链式任务中前一个任务的返回值默认是下一个任务的输入值之一 ( 不想让返回值做默认参数可以用 si() 或者 s(immutable=True) 的方式调用 )。
    chain = update_pl_all_task.s() | update_compare_pl_all_task.si()
    chain()


# 测试使用
# @celery.task()
# def add_together_task(a, b):
#     logger.info("%d + %d = %d", a, b, a + b)
#     return a + b


# 测试使用
# @celery.task()
# def print_add_result(c):
#     logger.info("add result: %d", c)


# 测试使用 chain
# @celery.task()
# def chain_task_test(a, b):
#     # fetch_page -> parse_page -> store_page
#     chain = add_together_task.s(a, b) | print_add_result.s()
#     chain()


# 测试使用 定时任务
# @celery.task(bind=True)
# def period_task(self):
#     print('period task done: {0}'.format(self.request.id))


if __name__ == "__main__":
    import time
    result = chain_task.delay()
    time.sleep(3)
    # result = update_compare_pl_all_task.delay()
    result.wait(3)
    logger.info('test finished')

