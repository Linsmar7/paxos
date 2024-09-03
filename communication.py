import numpy

# Envia mensagem com probabilidade de falha
def enviarComFalha(conteudo, id_remetente, id_destino, prob_falha, socket_envio):
  ocorreu_falha = numpy.random.choice([True, False], p=[prob_falha, 1 - prob_falha])

  mensagem = {}
  if ocorreu_falha:
    mensagem = {"conteudo": f"FALHA {id_remetente}", "de": id_remetente, "para": id_destino}
  else:
    mensagem = {"conteudo": conteudo, "de": id_remetente, "para": id_destino}

  socket_envio.send_json(mensagem)


# Envia mensagem para todos com probabilidade de falha
def broadcastComFalha(conteudo, id_remetente, num_processos, prob_falha, dict_sockets_envio):
  for id_destino in range(num_processos):
    socket_envio = dict_sockets_envio[id_destino]
    enviarComFalha(conteudo, id_remetente, id_destino, prob_falha, socket_envio)


# Envia mensagem sem probabilidade de falha
def enviarSemFalha(conteudo, id_remetente, id_destino, socket_envio):
  mensagem = {"conteudo": conteudo, "de": id_remetente, "para": id_destino}
  socket_envio.send_json(mensagem)


# Envia mensagem para todos sem probabilidade de falha
def broadcastSemFalha(conteudo, id_remetente, num_processos, dict_sockets_envio, excluir_remetente=False):
  for id_destino in range(num_processos):
    socket_envio = dict_sockets_envio[id_destino]

    if excluir_remetente and id_destino == id_remetente:
      continue

    enviarSemFalha(conteudo, id_remetente, id_destino, socket_envio)
