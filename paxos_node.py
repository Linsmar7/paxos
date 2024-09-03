import time
import zmq
from communication import enviarComFalha, enviarSemFalha, broadcastComFalha, broadcastSemFalha
from config import BASE_PORT

def PaxosNos(id_no, valor, numProc, prob, numRodadas, barreira):
  rodadaMaxVotada = -1  # Proposer e Acceptor
  valorMaxVotado = None  # Proposer e Acceptor
  valorProposto = None  # Proposer
  decisao = None  # Proposer

  # Cria socket PULL (1 socket) (usa bind, pois ele receberá mensagens de N nós)
  contexto = zmq.Context()
  socket_pull = contexto.socket(zmq.PULL)
  socket_pull.bind(f"tcp://127.0.0.1:{BASE_PORT + id_no}")

  # Cria sockets PUSH (N sockets) (usa connect pois eles serão usados ​​para enviar 1 mensagem)
  dict_sockets_push = {}

  for id_destino in range(numProc):
    socket_push = contexto.socket(zmq.PUSH)
    socket_push.connect(f"tcp://127.0.0.1:{BASE_PORT + id_destino}")
    dict_sockets_push[id_destino] = socket_push

  # Espera conexões
  time.sleep(0.3)

  for r in range(numRodadas):
    e_proposer = r % numProc == id_no

    if e_proposer:
      print(f"RODADA {r} INICIADA COM VALOR INICIAL: {valor}")
      time.sleep(0.3)

      broadcastComFalha(
        conteudo="INICIAR",
        id_remetente=id_no,
        num_processos=numProc,
        prob_falha=prob,
        dict_sockets_envio=dict_sockets_push,
      )

    # Recebe 'INICIAR|FALHA' do proposer
    mensagem_recebida = socket_pull.recv_json()

    # Analisa a mensagem recebida
    corpo_mensagem_recebida = mensagem_recebida["conteudo"]
    remetente_mensagem_recebida = mensagem_recebida["de"]

    time.sleep(0.3)

    # Fase1 --------------------------------------------------
    contagem_juncao = 0
    ira_propor = False

    if e_proposer:
      print(
        f"LÍDER {id_no} RECEBEU NA FASE DE JOIN: {corpo_mensagem_recebida}"
      )

      start_recebido = False

      if "INICIAR" in corpo_mensagem_recebida:
        contagem_juncao += 1
        start_recebido = True

      # Recebe respostas de N-1 acceptors ('JOIN|FALHA')
      rodadaMaxVotadaRecebida = -1
      valorMaxVotadoRecebido = -1

      for _ in range(numProc - 1):
        mensagem_recebida = socket_pull.recv_json()

        # Analisa a mensagem recebida
        corpo_mensagem_recebida = mensagem_recebida["conteudo"]
        remetente_mensagem_recebida = mensagem_recebida["de"]

        print(
          f"LÍDER {id_no} RECEBEU NA FASE DE JOIN: {corpo_mensagem_recebida}"
        )

        if "JOIN" in corpo_mensagem_recebida:
          contagem_juncao += 1

          # Mensagem recebida "JOIN {rodadaMaxVotada} {valorMaxVotado}"
          join_analisado = corpo_mensagem_recebida.split(" ")

          if int(join_analisado[1]) > rodadaMaxVotadaRecebida:
            rodadaMaxVotadaRecebida = int(join_analisado[1])
            valorMaxVotadoRecebido = int(join_analisado[2])

      # Se a maioria fez join
      if contagem_juncao > int(numProc / 2):
        ira_propor = True

        if start_recebido:
          if rodadaMaxVotada == -1:
            valorProposto = valor
          else:
            valorProposto = valorMaxVotadoRecebido
        else:
          valorProposto = valor

      else:
        ira_propor = False

    elif not e_proposer:
      # Como acceptor:
      print(f"ACCEPTOR {id_no} RECEBEU NA FASE DE JOIN: {corpo_mensagem_recebida}")

      if "INICIAR" in corpo_mensagem_recebida:
        time.sleep(0.3)

        # Envia "JOIN" ao proposer
        enviarComFalha(
          conteudo=f"JOIN {rodadaMaxVotada} {valorMaxVotado}",
          id_remetente=id_no,
          id_destino=remetente_mensagem_recebida,
          prob_falha=prob,
          socket_envio=dict_sockets_push[remetente_mensagem_recebida],
        )

      elif "FALHA" in corpo_mensagem_recebida:
        # Se um acceptor recebe 'FALHA', ele responde com 'FALHA'
        enviarSemFalha(
          conteudo=f"FALHA {id_no}",
          id_remetente=id_no,
          id_destino=remetente_mensagem_recebida,
          socket_envio=dict_sockets_push[remetente_mensagem_recebida],
        )

    barreira.wait()
    # Fase2 --------------------------------------------------
    if e_proposer:
      # Como proposer
      time.sleep(0.3)

      if ira_propor:
        # Broadcast 'PROPOSE'
        broadcastComFalha(
          conteudo=f"PROPOSE {valorProposto}",
          id_remetente=id_no,
          num_processos=numProc,
          prob_falha=prob,
          dict_sockets_envio=dict_sockets_push,
        )
      else:
        # Broadcast 'MUDARODADA'
        print(f"LÍDER DA RODADA {r} MUDOU DE RODADA")
        broadcastSemFalha(
          conteudo="MUDARODADA",
          id_remetente=id_no,
          num_processos=numProc,
          dict_sockets_envio=dict_sockets_push,
        )

    # Recebe 'PROPOSE|FALHA|MUDARODADA' do proposer
    mensagem_recebida = socket_pull.recv_json()

    # Analisa a mensagem recebida
    corpo_mensagem_recebida = mensagem_recebida["conteudo"]
    remetente_mensagem_recebida = mensagem_recebida["de"]

    if e_proposer:
      # Como proposer
      if ira_propor:
        # Se o proposer propôs novo valor, ele recebe N respostas
        contagem_votos = 0
        propose_recebido = False

        if "MUDARODADA" not in corpo_mensagem_recebida:
          print(
            f"LÍDER {id_no} RECEBEU NA FASE DE VOTAÇÃO: {corpo_mensagem_recebida}"
          )

          if "PROPOSE" in corpo_mensagem_recebida:
            contagem_votos += 1
            propose_recebido = True

          # Recebe respostas de n-1 acceptors ('JOIN|FALHA')
          for _ in range(numProc - 1):
            mensagem_recebida = socket_pull.recv_json()

            # Analisa a mensagem recebida
            corpo_mensagem_recebida = mensagem_recebida["conteudo"]
            remetente_mensagem_recebida = mensagem_recebida["de"]

            print(
                f"LÍDER {id_no} RECEBEU NA FASE DE VOTAÇÃO: {corpo_mensagem_recebida}"
            )

            if "VOTO" in corpo_mensagem_recebida:
                contagem_votos += 1

          if propose_recebido:
            rodadaMaxVotada = r
            valorMaxVotado = valorProposto

          if contagem_votos > int(numProc / 2):
            decisao = valorProposto
            print(f"LÍDER {id_no} DECIDIU PELO VALOR: {decisao}")

      pass

    elif not e_proposer:
      # Como acceptor
      time.sleep(0.3)
      print(f"ACCEPTOR {id_no} RECEBEU NA FASE DE VOTAÇÃO: {corpo_mensagem_recebida}")

      if "PROPOSE" in corpo_mensagem_recebida:
        # envia 'VOTO' como acceptor
        enviarComFalha(
          conteudo="VOTO",
          id_remetente=id_no,
          id_destino=remetente_mensagem_recebida,
          prob_falha=prob,
          socket_envio=dict_sockets_push[remetente_mensagem_recebida],
        )
        rodadaMaxVotada = r
        valorMaxVotado = int(corpo_mensagem_recebida.split(" ")[1])

      elif "MUDARODADA" in corpo_mensagem_recebida:
        pass
      elif "FALHA" in corpo_mensagem_recebida:
        # Se um acceptor recebe 'FALHA', ele responde com 'FALHA'
        enviarSemFalha(
          conteudo=f"FALHA {id_no}",
          id_remetente=id_no,
          id_destino=remetente_mensagem_recebida,
          socket_envio=dict_sockets_push[remetente_mensagem_recebida],
        )

      pass

    barreira.wait()
    time.sleep(0.3)

  pass
