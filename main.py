import json
import time
import pika
import threading as th
import multiprocessing as mp

# Thread que fica bloqueada recebendo mensagens do Rabbit
def rabbit_connection(config, event_queue):
    while True:
        if event_queue.qsize() > 0:
            event = event_queue.get()
            if event['msg_type'] == 'test':
                print('Thread: {name}, Test event: {event}'.format(name= th.get_ident(),event=event))
            if event['msg_type'] == 'end':
                print('Thread: {name}, End event: {event}'.format(name=th.get_ident(), event=event))
                break
    print('Thread {} killed'.format(th.get_ident()))

# Thread de controle que recebe os eventos
def controller_thread(main_context_queue, rabbit_threads):
    while True:
        if main_context_queue.qsize() > 0:
            msg = main_context_queue.get()
            if msg['msg_type'] == 'test':
                rabbit_threads[msg['msg']]['event_queue'].put(msg)
            elif msg['msg_type'] == 'create':
                pass
            elif msg['msg_type'] == 'end':
                print('Matar a thread: {thread}'.format(thread=msg['msg']))
                rabbit_threads[msg['msg']]['event_queue'].put(msg)
        else:
            time.sleep(1)

# Processo que controla o rabbit. Uma thread recebe eventos e as outras fazem a conexão com o Rabbit
def rabbit_main(main_context_queue, rabbit_config):

    print('Threads started')

    rabbit_threads = {}
    for config in rabbit_config:
        exchange_name = config['exchange']
        rabbit_threads[exchange_name] = {}
        event_queue = mp.Queue()
        rabbit_threads[exchange_name]['thread'] = th.Thread(target=rabbit_connection, args=(config, event_queue,), name=exchange_name)
        rabbit_threads[exchange_name]['event_queue'] = event_queue

    controller = th.Thread(target=controller_thread, args=(main_context_queue, rabbit_threads,))

    controller.start()
    for config in rabbit_config:
        rabbit_threads[config['exchange']]['thread'].start()

    controller.join()
    for config in rabbit_config:
        rabbit_threads[config['exchange']]['thread'].join()

    print('Threads ended')


#Processo principal que envia mensagens na fila principal para os outros processos pegarem
def main(main_context_queue):

    time.sleep(2)
    msg = {'timestamp': time.time(), 'msg_type': 'test', 'msg': 'exchange1'}
    main_context_queue.put(msg)

    time.sleep(2)
    msg = {'timestamp': time.time(), 'msg_type': 'end', 'msg': 'exchange2'}
    main_context_queue.put(msg)


if __name__ == '__main__':
#   Cria um processo principal e um processo que controla apenas o Rabbit
    rabbit_config = [
        {'host': None, 'exchange': 'exchange1', 'queue': None},
        {'host': None, 'exchange': 'exchange2', 'queue': None},
    ]
    main_context_queue = mp.Queue()

    main_context = mp.Process(target=main, args=(main_context_queue,))
    rabbit_process = mp.Process(target=rabbit_main, args=(main_context_queue, rabbit_config,))
    
    main_context.start()
    rabbit_process.start()
    
    main_context.join()
    rabbit_process.join()