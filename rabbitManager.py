import pika
import sys
import os
import json
import uuid
from datetime import datetime

username = os.environ['RABBITMQ_USER']
password = os.environ['RABBITMQ_PASSWD']
host = os.environ['RABBITMQ_HOST']
port = os.environ['RABBITMQ_PORT']
vhost = os.environ['RABBITMQ_VHOST']
contador_de_acks = 0
contador_de_msgs = 0
contador_de_postagens = 0  # Contador de mensagens postadas com sucesso
one_file_per_msg = False
RED = "\033[91m"
RESET = "\033[0m"


def queue_exists(channel, queue_name):
    """Verifica se a fila existe."""
    try:
        channel.queue_declare(queue=queue_name, passive=True)
        return True
    except pika.exceptions.ChannelClosedByBroker:
        return False

def generate_individual_file(body, filename, directory=None):
    if directory:
        createFile(filename, body, directory)
    else:    
        createFile(filename, body)

# Função para enviar mensagens a partir de um arquivo JSON
def send_messages_from_file(channel, queue_name, file_path):
    global contador_de_postagens
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            messages = json.load(f)
            for message in messages:
                body = json.dumps(message)
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=body,
                    properties=pika.BasicProperties(
                        message_id=str(uuid.uuid4())
                    )
                )
                contador_de_postagens += 1
                print(f"Mensagem enviada com sucesso: {body}")
    except Exception as e:
        print(f"Erro ao ler ou enviar mensagens: {e}")

# metodo chamado apenas quando é passado o parametro com o nome do arquivo de output
# Objetivo: imprimir e gravar no arquivo json, todas as mensagens de uma fila sem removê-las.
def callback_backup(ch, method, properties, body, queue_name):
    global contador_de_msgs
    global file_body

    message_id = properties.message_id
    if (message_id):
        filename = f"{message_id}.json"
    else:
        filename = f"{contador_de_msgs+1}.json"

    body = body.decode('utf-8')
    print(f"{body}")
    file_body += str(body)+','+'\n'
    if (one_file_per_msg):
        generate_individual_file(str(body), filename, queue_name) 
    contador_de_msgs += 1

# metodo chamado apenas quando é passado o parametro com o nome da fila
def callback_read_one_by_one(ch, method, properties, body):
    global RED
    global RESET
    global contador_de_acks
    global contador_de_msgs
    body = body.decode('utf-8')
    print(f"{RED}{body}{RESET}")
    contador_de_msgs += 1

    user_input = input("Deseja dar ACK para esta mensagem? (s/n): ")
    if user_input.lower() == 's':
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("ACK dado.")
        contador_de_acks += 1
    else:
        print("Mensagem não confirmada.")

def show_help():
    print("Uso: rabbitManager <nome_da_fila> [backup | lista.json] <--auto_ack | --one-file-per-msg>")
    print("")
    print("Exemplos de uso:")
    print("  1. rabbitManager fila")
    print("     Este é o cenário padrão que mostra todas as mensagens da fila e o usuário escolhe se quer dar ACK. Se quiser mudar a lógica, basta trabalhar dentro do método callback_read_one_by_one.")
    print("")
    print("  2. rabbitManager fila backup")
    print("     Use este cenário se quiser criar um arquivo com todas as mensagens da fila removendo-as. Neste exemplo seria criado o arquivo json com uma lista de mensagens atualmente existentes na fila informada.")
    print("")
    print("  3. rabbitManager fila backup --auto-ack")
    print("     Mesmo cenário anterior, porém removendo a mensagem da fila.")
    print("")
    print("  4. rabbitManager fila backup --one-file-per-msg")
    print("     Mesmo cenário anterior, porém além de criar um arquivo json com a lista de mensagens, cria arquivos json individuais por msg.")
    print("")
    print("  4. rabbitManager fila lista.json")
    print("     Use este cenário se quiser postar mensagens em uma fila. O arquivo JSON deve conter uma lista de mensagens, como mostrado no exemplo abaixo.")
    print("     Exemplo de lista.json:")
    print("     [")
    print("       {\"param1\": \"1a\"},")
    print("       {\"param2\": \"abc\"}")
    print("     ]")
    sys.exit()


def main(queue_name, second_arg, auto_ack):

    global contador_de_acks
    global contador_de_msgs
    global contador_de_postagens
    global file_body

    credentials = pika.PlainCredentials(username, password)
    connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=credentials
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    # Verifica se a fila existe
    if not queue_exists(channel, queue_name):
        print(f"A fila '{queue_name}' não existe.")
        connection.close()
        return

    if second_arg=='backup':
        file_body = '[\n'
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body: callback_backup(ch, method, properties, body, queue_name),
            auto_ack=auto_ack
        )
    elif second_arg.endswith('.json'):
        send_messages_from_file(channel, queue_name, second_arg)
        connection.close()
        return
    else:
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body: callback_read_one_by_one(ch, method, properties, body),
            auto_ack=False
        )

    print(f'Aguardando mensagens na fila "{queue_name}". Para sair pressione CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Parando o consumidor.")   
    finally:
        if contador_de_acks >= 0:
            print(f"{contador_de_acks} mensagens removidas da fila.")
        print(f"{contador_de_msgs} mensagens na fila.")
        print(f"{contador_de_postagens} mensagens enviadas com sucesso.")
        connection.close()
        if second_arg == 'backup':
            if file_body.endswith(',\n'):
                file_body = file_body[:-2]
                file_body = file_body+'\n]'
            data_formatada = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = 'payloads'+data_formatada+'.json'
            createFile(filename, file_body)
            createFile('payloads.json', file_body, queue_name)

def createFile(filename, body, directory=None):
    if directory:
        os.makedirs(directory, exist_ok=True)
        filename = os.path.join(directory, filename) 
    with open(filename, 'w', encoding='utf-8') as final_file:
        final_file.write(body)

def isParameter(arg):
    return True if (arg[:2] == '--') and parameterValidate(arg) else False

def parameterValidate(param):
    return True if (isAutoAck(param) or isOneFilePerMsg(param)) else False

def isAutoAck(param):
    return True if param == '--auto-ack' else False

def isOneFilePerMsg(param):
    return True if param == '--one-file-per-msg' else False    

if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 5:
        show_help()
    else:
        queue_name = ''
        second_arg=None
        auto_ack=None

        match len(sys.argv):
            case 2:
                if (not isParameter(sys.argv[1])):
                    queue_name = sys.argv[1]
                    second_arg = ''
                else:
                    show_help()
            case 3:
                if (not isParameter(sys.argv[1]) and not isParameter(sys.argv[2])):
                    queue_name = sys.argv[1]
                    second_arg = sys.argv[2]
                else:
                    show_help()
            case 4:    
                if (not isParameter(sys.argv[1]) and not isParameter(sys.argv[2]) and isParameter(sys.argv[3])):
                    queue_name = sys.argv[1]
                    second_arg = sys.argv[2]
                    auto_ack = isAutoAck(sys.argv[3])
                    one_file_per_msg = isOneFilePerMsg(sys.argv[3])
                else:
                    show_help()
            case 5:
                if (not isParameter(sys.argv[1]) and not isParameter(sys.argv[2])):
                    if (isParameter(sys.argv[3]) and isParameter(sys.argv[4])):
                        queue_name = sys.argv[1]
                        second_arg = sys.argv[2]
                        auto_ack = True if isAutoAck(sys.argv[3]) else True if isAutoAck(sys.argv[4]) else False
                        one_file_per_msg = True if isOneFilePerMsg(sys.argv[3]) else True if isOneFilePerMsg(sys.argv[4]) else False
                    elif (isParameter(sys.argv[3])):
                        queue_name = sys.argv[1]
                        second_arg = sys.argv[2]
                        auto_ack = isAutoAck(sys.argv[3])
                        one_file_per_msg = isOneFilePerMsg(sys.argv[3])
                    elif (isParameter(sys.argv[4])):
                        queue_name = sys.argv[1]
                        second_arg = sys.argv[2]
                        auto_ack = isAutoAck(sys.argv[4])
                        one_file_per_msg = isOneFilePerMsg(sys.argv[4])
                    else:
                        show_help()   
        main(queue_name, second_arg, auto_ack)

