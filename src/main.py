# Import packages
import os
from InvestBoard import InvestBoard


def main():
    # Load data
    path = os.path.split(os.getcwd())[0]
    path = os.path.join(path, 'data\\user_data.json')

    app = InvestBoard(data_path=path)
    app.start()


if __name__ == "__main__":
    main()
