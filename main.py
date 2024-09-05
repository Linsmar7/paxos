import sys
import random
from multiprocessing import Process, Barrier
from paxos_node import PaxosNos


"""Cria e retorna uma lista de processos, cada um representando um nó Paxos."""
def criar_processos(num_proc, prob_falha, num_rodadas, barreira):
  processos = []
  for id_no in range(num_proc):
    valor_inicial = random.randint(0, 100)
    processo = Process(
      target=PaxosNos,
      args=(id_no, valor_inicial, num_proc, prob_falha, num_rodadas, barreira)
    )
    processos.append(processo)
  return processos


"""Inicia todos os processos na lista fornecida."""
def iniciar_processos(processos):
  for processo in processos:
    processo.start()


"""Aguarda até que todos os processos na lista fornecida terminem."""
def aguardar_termino(processos):
  for processo in processos:
    processo.join()


def main(args):
  if len(args) != 4:
    print("Argumentos de linha de comando inválidos! Uso: <num_proc> <prob_falha> <num_rodadas>")
    return

  num_proc = int(args[1])
  prob_falha = float(args[2])
  num_rodadas = int(args[3])

  barreira = Barrier(num_proc)

  print(f"NÚMERO DE NÓS: {num_proc}, PROBABILIDADE DE FALHA: {prob_falha}, NÚMERO DE RODADAS: {num_rodadas}")

  processos = criar_processos(num_proc, prob_falha, num_rodadas, barreira)
  iniciar_processos(processos)
  aguardar_termino(processos)

if __name__ == "__main__":
  main(sys.argv)
