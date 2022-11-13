from mrjob.job import MRJob
import re
 

# A linha de código a seguir recebe uma linha de texto como entrada e retorna uma lista de palavras sem os espaços.

palavra_regex = re.compile(r'[\w']+')

class QuantidadePalavras(MRJob):

    def mapper(self, _, linha):
        for palavra in palavra_regex.findall(linha):
            yield palavra.lower(), 1

    def reducer(self, palavra, ocorrencias):
        yield palavra, sum(ocorrencias)