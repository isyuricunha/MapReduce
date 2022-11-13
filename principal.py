from mrjob.job import MRJob
import re

palavra_regex = re.compile(r'[\w]+')


class QuantidadePalavras(MRJob):
    def mapper(self, _, linha):
        for p in palavra_regex.findall(linha):
            yield p.lower(), 1

    def reducer(self, p, qtd):
        yield p, sum(qtd)


if __name__ == '__main__':
    QuantidadePalavras.run()