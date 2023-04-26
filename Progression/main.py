from controller_worker import ControllerWorker
from progression_table_processor import ProgressionTableProcessor


if __name__ == '__main__':

    main = ProgressionTableProcessor()

    controller = ControllerWorker(main)
    controller.start()

    main.start_process()



