import warnings

warnings.filterwarnings("ignore")
warnings.showwarning = lambda *args, **kwargs: None

from .main import main

main()
