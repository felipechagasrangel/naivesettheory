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
            if event == 'test':
                print('Test event: {event}'.format(event=event))
            elif event == 'end':
                print('End event: {event}'.format(event=event))

# Thread de controle que recebe os eventos
def controller_thread(main_context_queue, event_queue):
    while True:
        if main_context_queue.qsize() > 0:
            msg = main_context_queue.get()
            event_queue.put(msg['msg_type'])
        else:
            time.sleep(1)

# Processo que controla o rabbit. Uma thread recebe eventos e as outras fazem a conexão com o Rabbit
def rabbit_main(main_context_queue, rabbit_config):
    event_queue = mp.Queue()
    controller = th.Thread(target=controller_thread, args=(main_context_queue, event_queue,))

    print('Threads started')

    rabbit_threads = {}
    for config in rabbit_config:
        rabbit_threads[config['exchange']] = th.Thread(target=rabbit_connection, args=(config, event_queue,))

    controller.start()
    for config in rabbit_config:
        rabbit_threads[config['exchange']].start()

    controller.join()
    for config in rabbit_config:
        rabbit_threads[config['exchange']].join()

    print('Threads ended')


#Processo principal que envia mensagens na fila principal para os outros processos pegarem
def main(main_context_queue):

    time.sleep(2)
    msg = {'timestamp': time.time(), 'msg_type': 'test', 'msg': None}
    main_context_queue.put(msg)

    time.sleep(2)
    msg = {'timestamp': time.time(), 'msg_type': 'end', 'msg': None}
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