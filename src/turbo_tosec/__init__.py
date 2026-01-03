# __init__.py

# Dışarıya açmak istediğimiz sınıfları burada "export" ediyoruz.
# Böylece kullanıcı "from turbo_tosec.parser import TurboParser" demek yerine
# direkt "from turbo_tosec import TurboParser" diyebilecek.

from turbo_tosec.parser import TurboParser, InMemoryParser
from turbo_tosec.session import ImportSession
from turbo_tosec.database import DatabaseManager # Eğer lazımsa
from turbo_tosec._version import __version__

# Versiyon bilgisi (Opsiyonel ama şıktır)
__version__ = __version__
__author__ = "DeponesLabs"