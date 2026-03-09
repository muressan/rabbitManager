import argparse
import json
import os
import sys
import uuid
from datetime import datetime

import pika

username = os.environ['RABBITMQ_USER']
password = os.environ['RABBITMQ_PASSWD']
host = os.environ['RABBITMQ_HOST']
port = int(os.environ['RABBITMQ_PORT'])
vhost = os.environ['RABBITMQ_VHOST']

contador_de_acks = 0
contador_de_msgs = 0
contador_de_postagens = 0
one_file_per_msg = False

DEFAULT_BATCH_SIZE = 500
DEFAULT_PREFETCH_COUNT = 100
RED = "\033[91m"
RESET = "\033[0m"


def positive_int(value):
    int_value = int(value)
    if int_value <= 0:
        raise argparse.ArgumentTypeError("O valor deve ser um inteiro maior que zero.")
    return int_value


def queue_exists(connection, queue_name):
    """Verifica se a fila existe sem invalidar o canal principal."""
    probe_channel = connection.channel()
    try:
        probe_channel.queue_declare(queue=queue_name, passive=True)
        return True
    except pika.exceptions.ChannelClosedByBroker:
        return False
    finally:
        if probe_channel.is_open:
            probe_channel.close()


def exchange_exists(connection, exchange_name, exchange_type):
    """Verifica se a exchange existe com o tipo informado."""
    probe_channel = connection.channel()
    try:
        probe_channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, passive=True)
        return True
    except pika.exceptions.ChannelClosedByBroker:
        return False
    finally:
        if probe_channel.is_open:
            probe_channel.close()


def create_file(filename, body, directory=None):
    if directory:
        os.makedirs(directory, exist_ok=True)
        filename = os.path.join(directory, filename)
    with open(filename, 'w', encoding='utf-8') as final_file:
        final_file.write(body)
        final_file.flush()
        os.fsync(final_file.fileno())


def generate_individual_file(body, filename, directory=None):
    if directory:
        create_file(filename, body, directory)
    else:
        create_file(filename, body)


def normalize_json_line(body):
    decoded_body = body.decode('utf-8')
    try:
        parsed_body = json.loads(decoded_body)
        return json.dumps(parsed_body, ensure_ascii=False)
    except json.JSONDecodeError:
        return json.dumps({'body': decoded_body}, ensure_ascii=False)


class BackupWriter:
    def __init__(self, queue_name, batch_size, ack_mode, write_individual_files):
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.ack_mode = ack_mode
        self.write_individual_files = write_individual_files
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.base_directory = os.path.join(queue_name, f"backup_{timestamp}")
        self.individual_directory = os.path.join(self.base_directory, 'messages')
        os.makedirs(self.base_directory, exist_ok=True)
        if self.write_individual_files:
            os.makedirs(self.individual_directory, exist_ok=True)
        self.part_number = 1
        self.messages_in_current_file = 0
        self.current_file = None
        self.current_final_path = ''
        self.current_working_path = ''
        self.pending_batch_count = 0
        self.pending_last_delivery_tag = None

    def _open_new_file(self):
        file_name = f"part-{self.part_number:06d}.jsonl"
        self.current_final_path = os.path.join(self.base_directory, file_name)
        if self.ack_mode == 'batch':
            self.current_working_path = f"{self.current_final_path}.tmp"
        else:
            self.current_working_path = self.current_final_path
        self.current_file = open(self.current_working_path, 'a', encoding='utf-8')
        self.messages_in_current_file = 0

    def _fsync_current_file(self):
        self.current_file.flush()
        os.fsync(self.current_file.fileno())

    def _ack_pending_batch(self, channel):
        global contador_de_acks
        if self.pending_last_delivery_tag is None or self.pending_batch_count == 0:
            return
        channel.basic_ack(delivery_tag=self.pending_last_delivery_tag, multiple=True)
        contador_de_acks += self.pending_batch_count
        self.pending_last_delivery_tag = None
        self.pending_batch_count = 0

    def _close_current_file(self):
        if self.current_file is None:
            return
        self._fsync_current_file()
        self.current_file.close()
        if self.ack_mode == 'batch':
            os.replace(self.current_working_path, self.current_final_path)
        print(f"Arquivo gerado: {self.current_final_path}")
        self.current_file = None
        self.current_final_path = ''
        self.current_working_path = ''
        self.messages_in_current_file = 0
        self.part_number += 1

    def _rotate_file(self, channel):
        self._close_current_file()
        if self.ack_mode == 'batch':
            self._ack_pending_batch(channel)

    def write_message(self, channel, method, properties, body, message_index):
        global contador_de_acks
        global contador_de_msgs
        if self.current_file is None:
            self._open_new_file()
        json_line = normalize_json_line(body)
        self.current_file.write(json_line + '\n')
        self._fsync_current_file()
        if self.write_individual_files:
            message_id = properties.message_id or str(message_index)
            generate_individual_file(json_line, f"{message_id}.json", self.individual_directory)
        if self.ack_mode == 'message':
            channel.basic_ack(delivery_tag=method.delivery_tag)
            contador_de_acks += 1
        elif self.ack_mode == 'batch':
            self.pending_last_delivery_tag = method.delivery_tag
            self.pending_batch_count += 1
        contador_de_msgs += 1
        self.messages_in_current_file += 1
        if self.messages_in_current_file >= self.batch_size:
            self._rotate_file(channel)

    def close(self, channel):
        if self.current_file is not None:
            self._close_current_file()
        if self.ack_mode == 'batch':
            self._ack_pending_batch(channel)


def send_messages_from_file(channel, file_path, exchange_name='', routing_key=''):
    global contador_de_postagens
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            messages = json.load(file)
        for message in messages:
            body = json.dumps(message, ensure_ascii=False)
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(message_id=str(uuid.uuid4()))
            )
            contador_de_postagens += 1
            print(f"Mensagem enviada com sucesso: {body}")
    except Exception as exc:
        print(f"Erro ao ler ou enviar mensagens: {exc}")


def callback_read_one_by_one(ch, method, properties, body):
    global contador_de_acks
    global contador_de_msgs
    decoded_body = body.decode('utf-8')
    print(f"{RED}{decoded_body}{RESET}")
    contador_de_msgs += 1
    user_input = input("Deseja dar ACK para esta mensagem? (s/n): ").strip().lower()
    if user_input == 's':
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("ACK dado.")
        contador_de_acks += 1
    else:
        print("Mensagem não confirmada.")


def run_backup(channel, queue_name, batch_size, prefetch_count, ack_mode, write_individual_files):
    writer = BackupWriter(queue_name, batch_size, ack_mode, write_individual_files)
    channel.basic_qos(prefetch_count=prefetch_count)
    print(f'Iniciando backup da fila "{queue_name}"')
    print(f"Diretório de saída: {writer.base_directory}")
    print(f"batch-size: {batch_size}")
    print(f"prefetch-count: {prefetch_count}")
    print(f"ack-mode: {ack_mode}")
    auto_ack = ack_mode == 'none'
    try:
        message_index = 1
        for method, properties, body in channel.consume(queue=queue_name, inactivity_timeout=1, auto_ack=auto_ack):
            if method is None:
                break
            writer.write_message(channel, method, properties, body, message_index)
            message_index += 1
    except KeyboardInterrupt:
        print("Parando o backup.")
    finally:
        writer.close(channel)
        try:
            channel.cancel()
        except Exception:
            pass
    return writer.base_directory


def show_help():
    print("Uso:")
    print("  rabbitManager <nome_da_fila>")
    print("  rabbitManager <nome_da_fila> backup [--batch-size <n>] [--prefetch-count <n>] [--ack-mode <message|batch>] [--no-ack] [--one-file-per-msg]")
    print("  rabbitManager <nome_da_fila> <lista.json>")
    print("  rabbitManager --exchange <nome_da_exchange> --exchange-type <direct|topic|fanout> [--routing-key <routing_key>] <lista.json>")
    print("")
    print("Exemplos:")
    print("  1. rabbitManager fila")
    print("     Lê as mensagens uma a uma e pergunta se deve dar ACK.")
    print("")
    print("  2. rabbitManager fila backup")
    print(f"     batch-size padrão: {DEFAULT_BATCH_SIZE}")
    print(f"     prefetch-count padrão: {DEFAULT_PREFETCH_COUNT}")
    print("")
    print("  3. rabbitManager fila backup --batch-size 1000 --prefetch-count 200")
    print("     Mesmo cenário anterior, alterando o tamanho do lote e o prefetch.")
    print("")
    print("  4. rabbitManager fila backup --ack-mode batch")
    print("     Faz backup com ACK em lote. Exige confirmação explícita antes do consumo.")
    print("")
    print("  5. rabbitManager fila backup --no-ack")
    print("     Não recomendado. Use apenas se houver cerca de 100 mensagens na fila e se você aceitar que elas continuarão existindo na fila")
    print("")
    print("  6. rabbitManager fila backup --one-file-per-msg")
    print("     Além do .jsonl rotativo, cria arquivos JSON individuais por mensagem.")
    print("")
    print("  7. rabbitManager fila lista.json")
    print("     Posta mensagens em uma fila usando um arquivo JSON com uma lista de mensagens.")
    print("")
    print("  8. rabbitManager --exchange eventos.direct --exchange-type direct --routing-key cliente.rk lista.json")
    print("     Posta mensagens em uma exchange direct.")
    print("")
    print("  9. rabbitManager --exchange eventos.topic --exchange-type topic --routing-key cliente.* lista.json")
    print("     Posta mensagens em uma exchange topic.")
    print("")
    print("  10. rabbitManager --exchange eventos.broadcast --exchange-type fanout lista.json")
    print("     Posta mensagens em uma exchange fanout.")
    sys.exit()


def parse_arguments():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('positionals', nargs='*')
    parser.add_argument('--one-file-per-msg', action='store_true')
    parser.add_argument('--batch-size', type=positive_int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument('--prefetch-count', type=positive_int, default=DEFAULT_PREFETCH_COUNT)
    parser.add_argument('--ack-mode', choices=['message', 'batch'], default='message')
    parser.add_argument('--no-ack', action='store_true')
    parser.add_argument('--exchange')
    parser.add_argument('--exchange-type', choices=['direct', 'topic', 'fanout'])
    parser.add_argument('--routing-key')
    parser.add_argument('--help', action='store_true')
    args = parser.parse_args()

    ack_mode_setting = 'none' if args.no_ack else args.ack_mode
    if args.no_ack and args.ack_mode != 'message':
        show_help()
    if args.help:
        show_help()

    if args.exchange or args.exchange_type or args.routing_key:
        if not args.exchange or not args.exchange_type:
            show_help()
        if len(args.positionals) != 1 or not args.positionals[0].endswith('.json'):
            show_help()
        if args.one_file_per_msg or args.batch_size != DEFAULT_BATCH_SIZE or args.prefetch_count != DEFAULT_PREFETCH_COUNT or args.ack_mode != 'message' or args.no_ack:
            show_help()
        if args.exchange_type in ('direct', 'topic') and not args.routing_key:
            show_help()
        return {
            'queue_name': '',
            'second_arg': args.positionals[0],
            'exchange_name': args.exchange,
            'exchange_type': args.exchange_type,
            'routing_key': args.routing_key,
            'one_file_per_msg': False,
            'batch_size': DEFAULT_BATCH_SIZE,
            'prefetch_count': DEFAULT_PREFETCH_COUNT,
            'ack_mode': ack_mode_setting,
            'no_ack': False,
        }

    if len(args.positionals) < 1 or len(args.positionals) > 2:
        show_help()

    queue_name = args.positionals[0]
    second_arg = args.positionals[1] if len(args.positionals) == 2 else ''

    if second_arg == 'backup':
        return {
            'queue_name': queue_name,
            'second_arg': second_arg,
            'exchange_name': '',
            'exchange_type': '',
            'routing_key': '',
            'one_file_per_msg': args.one_file_per_msg,
            'batch_size': args.batch_size,
            'prefetch_count': args.prefetch_count,
            'ack_mode': ack_mode_setting,
            'no_ack': args.no_ack,
        }

    if second_arg.endswith('.json') or second_arg == '':
        if args.one_file_per_msg or args.batch_size != DEFAULT_BATCH_SIZE or args.prefetch_count != DEFAULT_PREFETCH_COUNT or args.ack_mode != 'message' or args.no_ack:
            show_help()
        return {
            'queue_name': queue_name,
            'second_arg': second_arg,
            'exchange_name': '',
            'exchange_type': '',
            'routing_key': '',
            'one_file_per_msg': False,
            'batch_size': DEFAULT_BATCH_SIZE,
            'prefetch_count': DEFAULT_PREFETCH_COUNT,
            'ack_mode': ack_mode_setting,
            'no_ack': False,
        }

    show_help()


def confirm_backup_execution(queue_name, batch_size, prefetch_count, ack_mode):
    print("")
    print("Aviso: este modo de backup remove mensagens da fila somente após persistência confirmada.")
    print("O modo mais seguro usa ACK manual por mensagem após gravação em disco.")
    print(f"Fila: {queue_name}")
    print(f"batch-size: {batch_size}")
    print(f"prefetch-count: {prefetch_count}")
    print(f"ack-mode: {ack_mode}")
    if ack_mode == 'batch':
        print("")
        print("ACK em lote pode causar reentrega ou perda parcial de trabalho em caso de falha entre persistência e confirmação.")
        print("Use apenas se aceitar esse risco operacional.")
    user_input = input("Deseja continuar? (s/n): ").strip().lower()
    return user_input == 's'


def main(queue_name, second_arg, exchange_name='', exchange_type='', routing_key='', batch_size=DEFAULT_BATCH_SIZE, prefetch_count=DEFAULT_PREFETCH_COUNT, ack_mode='message'):
    global contador_de_acks
    global contador_de_msgs
    global contador_de_postagens
    credentials = pika.PlainCredentials(username, password)
    connection_params = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    backup_output_directory = ''

    try:
        if second_arg == 'backup':
            if not queue_exists(connection, queue_name):
                print(f"A fila '{queue_name}' não existe.")
                return
            backup_output_directory = run_backup(
                channel=channel,
                queue_name=queue_name,
                batch_size=batch_size,
                prefetch_count=prefetch_count,
                ack_mode=ack_mode,
                write_individual_files=one_file_per_msg
            )
            return

        if second_arg.endswith('.json'):
            if exchange_name:
                if not exchange_exists(connection, exchange_name, exchange_type):
                    print(f"A exchange '{exchange_name}' do tipo '{exchange_type}' não existe.")
                    return
                send_messages_from_file(channel, second_arg, exchange_name=exchange_name, routing_key=routing_key or '')
            else:
                if not queue_exists(connection, queue_name):
                    print(f"A fila '{queue_name}' não existe.")
                    return
                send_messages_from_file(channel, second_arg, routing_key=queue_name)
            return

        if not queue_exists(connection, queue_name):
            print(f"A fila '{queue_name}' não existe.")
            return

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body: callback_read_one_by_one(ch, method, properties, body),
            auto_ack=False
        )
        print(f'Aguardando mensagens na fila "{queue_name}". Para sair pressione CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Parando o consumidor.")
    finally:
        print(f"{contador_de_acks} mensagens com ACK.")
        print(f"{contador_de_msgs} mensagens processadas.")
        print(f"{contador_de_postagens} mensagens enviadas com sucesso.")
        if backup_output_directory:
            print(f"Backup salvo em: {backup_output_directory}")
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    parsed_args = parse_arguments()
    one_file_per_msg = parsed_args['one_file_per_msg']
    if parsed_args['second_arg'] == 'backup':
        if not confirm_backup_execution(
            queue_name=parsed_args['queue_name'],
            batch_size=parsed_args['batch_size'],
            prefetch_count=parsed_args['prefetch_count'],
            ack_mode=parsed_args['ack_mode']
        ):
            print("Operação cancelada pelo usuário.")
            sys.exit(0)
    main(
        queue_name=parsed_args['queue_name'],
        second_arg=parsed_args['second_arg'],
        exchange_name=parsed_args['exchange_name'],
        exchange_type=parsed_args['exchange_type'],
        routing_key=parsed_args['routing_key'],
        batch_size=parsed_args['batch_size'],
        prefetch_count=parsed_args['prefetch_count'],
        ack_mode=parsed_args['ack_mode']
    )

