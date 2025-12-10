import sys
import psycopg2
from PyQt5.QtWidgets import *
from typing import List, Tuple, Optional

class ServiceDB:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: str = '5432'):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()
        print("Подключение к PostgreSQL установлено!")

    def execute_select(
            self,
            table: str,
            joins: Optional[List[Tuple[str, str]]] = None,
            fields: Optional[List[str]] = None,
            group_by: Optional[str] = None,
            order_by: Optional[str] = None,
            **where
    ) -> List[Tuple]:
        fields = ['*'] if fields is None else fields
        query = f'SELECT {", ".join(fields)} FROM {table}'

        if joins:
            for join_table, join_condition in joins:
                query += f' JOIN {join_table} ON {join_condition}'

        if where:
            where_conditions = []
            for x, y in where.items():
                if isinstance(y, str):
                    where_conditions.append(f"{x} = '{y}'")
                else:
                    where_conditions.append(f'{x} = {y}')
            query += ' WHERE ' + ' AND '.join(where_conditions)

        if group_by:
            query += f' GROUP BY {group_by}'

        if order_by:
            if order_by.startswith('-'):
                query += f' ORDER BY {order_by[1:]} DESC'
            else:
                query += f' ORDER BY {order_by} ASC'

        query += ';'
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Проверка существования дома по адресу
    def check_house_exists(self, address: str) -> Optional[int]:
        query = f"SELECT id FROM House WHERE address = '{address}'"
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result[0] if result else None

    def check_flat_exists(self, house_id: int, num_flat: int) -> bool:
        query = f"""
        SELECT EXISTS(
            SELECT 1 FROM Flat 
            WHERE id_house = {house_id} AND num_flat = {num_flat}
        )
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    # Получение списка существующих номеров квартир в доме
    def get_existing_flats(self, house_id: int) -> List[int]:
        query = f"SELECT num_flat FROM Flat WHERE id_house = {house_id}"
        self.cursor.execute(query)
        return [row[0] for row in self.cursor.fetchall()]

    def add_house(self, address: str, num_flats: int, description: str = "") -> int:
        query = f"""
        INSERT INTO House (address, num_flats, description) 
        VALUES ('{address}', {num_flats}, '{description}')
        RETURNING id
        """
        self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.fetchone()[0]

    def add_flat(self, house_id: int, num_flat: int, area: float) -> bool:
        if self.check_flat_exists(house_id, num_flat):
            raise ValueError(f"Квартира №{num_flat} уже существует в этом доме")

        query = f"""
        INSERT INTO Flat (num_flat, area, id_house) 
        VALUES ({num_flat}, {area}, {house_id})
        """
        self.cursor.execute(query)
        self.connection.commit()
        return True

    def close(self):
        self.cursor.close()
        self.connection.close()

# Диалоговое окно для добавления квартиры
class AddFlatDialog(QDialog):

    def __init__(self, db: ServiceDB, parent=None):
        super().__init__(parent)
        self.db = db
        self.setWindowTitle("Добавление квартиры")
        self.setModal(True)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Поле для ввода адреса дома
        address_layout = QHBoxLayout()
        address_layout.addWidget(QLabel("Адрес дома:"))
        self.address_input = QLineEdit()
        self.address_input.setPlaceholderText("ул. Ленина, д. 10")
        address_layout.addWidget(self.address_input)
        layout.addLayout(address_layout)

        # Кнопка проверки адреса
        check_button = QPushButton("Проверить адрес")
        check_button.clicked.connect(self.check_address)
        layout.addWidget(check_button)

        # Результат проверки
        self.address_status = QLabel("")
        layout.addWidget(self.address_status)

        # Поля для квартиры
        self.flat_group = QGroupBox("Данные квартиры")
        flat_layout = QVBoxLayout()

        # Номер квартиры
        num_layout = QHBoxLayout()
        num_layout.addWidget(QLabel("Номер квартиры:"))
        self.num_flat_input = QSpinBox()
        self.num_flat_input.setRange(1, 1000)
        num_layout.addWidget(self.num_flat_input)
        flat_layout.addLayout(num_layout)

        # Площадь
        area_layout = QHBoxLayout()
        area_layout.addWidget(QLabel("Площадь (м²):"))
        self.area_input = QDoubleSpinBox()
        self.area_input.setRange(10, 500)
        self.area_input.setDecimals(2)
        area_layout.addWidget(self.area_input)
        flat_layout.addLayout(area_layout)

        self.flat_group.setLayout(flat_layout)
        self.flat_group.setVisible(False)  # Скрываем до проверки адреса
        layout.addWidget(self.flat_group)

        # Кнопки
        button_layout = QHBoxLayout()
        self.add_button = QPushButton("Добавить квартиру")
        self.add_button.clicked.connect(self.add_flat)
        self.add_button.setEnabled(False)
        button_layout.addWidget(self.add_button)

        cancel_button = QPushButton("Отмена")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(cancel_button)

        layout.addLayout(button_layout)
        self.setLayout(layout)

    # Проверка существования дома по адресу
    def check_address(self):

        address = self.address_input.text().strip()
        if not address:
            QMessageBox.warning(self, "Ошибка", "Введите адрес дома")
            return

        try:
            house_id = self.db.check_house_exists(address)
            if house_id:
                self.address_status.setText(f"Дом найден (ID: {house_id})")
                self.address_status.setStyleSheet("color: green")
                self.flat_group.setVisible(True)
                self.add_button.setEnabled(True)
                self.house_id = house_id
            else:
                self.address_status.setText("Дом не найден")
                self.address_status.setStyleSheet("color: red")

                # Предложение создать новый дом
                reply = QMessageBox.question(
                    self,
                    "Дом не найден",
                    f"Дом по адресу '{address}' не найден.\nСоздать новый дом?",
                    QMessageBox.Yes | QMessageBox.No
                )

                if reply == QMessageBox.Yes:
                    self.create_new_house(address)
                else:
                    self.flat_group.setVisible(False)
                    self.add_button.setEnabled(False)

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при проверке адреса: {e}")

    # Создание диалога для добавления нового дома
    def create_new_house(self, address: str):
        dialog = QDialog(self)
        dialog.setWindowTitle("Создание нового дома")
        layout = QVBoxLayout()

        # Количество квартир
        flats_layout = QHBoxLayout()
        flats_layout.addWidget(QLabel("Количество квартир:"))
        num_flats_input = QSpinBox()
        num_flats_input.setRange(1, 1000)
        flats_layout.addWidget(num_flats_input)
        layout.addLayout(flats_layout)

        # Описание
        desc_layout = QVBoxLayout()
        desc_layout.addWidget(QLabel("Описание:"))
        desc_input = QTextEdit()
        desc_input.setMaximumHeight(100)
        desc_layout.addWidget(desc_input)
        layout.addLayout(desc_layout)

        # Кнопки
        button_layout = QHBoxLayout()
        ok_button = QPushButton("Создать")
        cancel_button = QPushButton("Отмена")

        def create_house():
            try:
                num_flats = num_flats_input.value()
                description = desc_input.toPlainText()

                house_id = self.db.add_house(address, num_flats, description)
                QMessageBox.information(
                    dialog,
                    "Успех",
                    f"Дом создан (ID: {house_id}). Теперь можно добавить квартиру."
                )

                self.house_id = house_id
                self.address_status.setText(f"Дом создан (ID: {house_id})")
                self.address_status.setStyleSheet("color: green")
                self.flat_group.setVisible(True)
                self.add_button.setEnabled(True)
                dialog.accept()

            except Exception as e:
                QMessageBox.critical(dialog, "Ошибка", f"Ошибка при создании дома: {e}")

        ok_button.clicked.connect(create_house)
        cancel_button.clicked.connect(dialog.reject)

        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

        dialog.setLayout(layout)
        dialog.exec_()

    def add_flat(self):
        try:
            num_flat = self.num_flat_input.value()
            area = self.area_input.value()

            # Дополнительная проверка перед добавлением
            if self.db.check_flat_exists(self.house_id, num_flat):
                QMessageBox.warning(
                    self,
                    "Ошибка",
                    f"Квартира №{num_flat} уже существует в этом доме!"
                )
                return

            self.db.add_flat(self.house_id, num_flat, area)

            QMessageBox.information(
                self,
                "Успех",
                f"Квартира №{num_flat} площадью {area}м^2 успешно добавлена!"
            )

            # Сбрасываем форму для добавления следующей квартиры
            self.num_flat_input.setValue(1)
            self.area_input.setValue(10.0)

            # Спрашиваем, добавить еще одну квартиру
            reply = QMessageBox.question(
                self,
                "Добавить еще",
                "Квартира добавлена успешно. Добавить еще одну квартиру в этот же дом?", QMessageBox.Yes | QMessageBox.No
            )

            if reply == QMessageBox.No:
                self.accept()

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при добавлении квартиры: {e}")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = ServiceDB(
            dbname='lab4',
            user='postgres',
            password='GGHHss89aa123'
        )

        self.menu_items = {
            1: 'Показать все дома',
            2: 'Показать все квартиры',
            3: 'Показать всех жильцов',
            4: 'Статистика платежей по домам',
            5: 'Статистика исполнителей',
            6: 'Рейтинги домов',
            7: 'Добавить квартиру',
            8: 'Выход'
        }

        self.menu = {
            1: (
                lambda: self.db.execute_select(
                    'House',
                    fields=['id', 'address', 'num_flats', 'description'],
                    order_by='address'
                ),
                ['ID', 'Адрес', 'Кол-во квартир', 'Описание']
            ),
            2: (
                lambda: self.db.execute_select(
                    'Flat f',
                    fields=['f.id', 'f.num_flat', 'f.area', 'h.address'],
                    joins=[('House h', 'f.id_house = h.id')],
                    order_by='h.address, f.num_flat'
                ),
                ['ID', 'Номер квартиры', 'Площадь', 'Адрес дома']
            ),
            3: (
                lambda: self.db.execute_select(
                    'Resident r',
                    fields=['r.FCS', 'f.num_flat', 'h.address', 'r.phone_number'],
                    joins=[
                        ('Flat f', 'r.id_flat = f.id'),
                        ('House h', 'f.id_house = h.id')
                    ],
                    order_by='r.FCS'
                ),
                ['ФИО', 'Номер квартиры', 'Адрес дома', 'Телефон']
            ),
            4: (
                lambda: self.db.execute_select(
                    'House h',
                    fields=['h.address', 'COUNT(fe.id)', 'COALESCE(SUM(fe.amount_fee), 0)'],
                    joins=[
                        ('Flat f', 'h.id = f.id_house'),
                        ('Payment p', 'f.id = p.id_flat'),
                        ('Fee fe', 'p.id = fe.id_payment')
                    ],
                    group_by='h.id, h.address',
                    order_by='-COALESCE(SUM(fe.amount_fee), 0)'
                ),
                ['Адрес дома', 'Кол-во платежей', 'Общая сумма']
            ),
            5: (
                lambda: self.db.execute_select(
                    'Executor e',
                    fields=['e.name', 'COUNT(cw.id)', 'SUM(cw.price)'],
                    joins=[('Completed_work cw', 'e.id = cw.id_executor')],
                    group_by='e.id, e.name',
                    order_by='-COUNT(cw.id)'
                ),
                ['Исполнитель', 'Количество работ', 'Общая стоимость']
            ),
            6: (
                lambda: self.db.execute_select(
                    'House h',
                    fields=['h.address', 'ROUND(AVG(p.rating), 2)', 'COUNT(p.id)'],
                    joins=[
                        ('Flat f', 'h.id = f.id_house'),
                        ('Payment p', 'f.id = p.id_flat')
                    ],
                    group_by='h.id, h.address'
                ),
                ['Адрес дома', 'Средний рейтинг', 'Кол-во счетов']
            ),
            7: (None, [])  # отдельная обработка
        }

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Жилищно-коммунальная компания - Лабораторная работа №6")
        self.setGeometry(100, 100, 1200, 600)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)

        # Панель кнопок
        toolbar = QHBoxLayout()

        for option, name in self.menu_items.items():
            if option == 8:  # Выход
                continue

            button = QPushButton(name)

            if option == 7:  # Добавить квартиру
                button.clicked.connect(self.add_flat_dialog)
            else:
                button.clicked.connect(lambda checked, opt=option: self.show_result(opt))

            toolbar.addWidget(button)

        # Кнопка выхода
        exit_btn = QPushButton('Выход')
        exit_btn.clicked.connect(self.quit_app)
        toolbar.addWidget(exit_btn)

        layout.addLayout(toolbar)

        # Таблица
        self.table = QTableWidget()
        layout.addWidget(self.table)

        # Статус
        self.status = QLabel('Готов к работе - выберите пункт меню')
        layout.addWidget(self.status)

    def show_result(self, option: int):
        if option not in self.menu:
            return

        func, columns = self.menu[option]

        try:
            data = func()

            self.table.setRowCount(len(data))
            self.table.setColumnCount(len(columns))
            self.table.setHorizontalHeaderLabels(columns)

            for row_idx, row in enumerate(data):
                for col_idx, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    self.table.setItem(row_idx, col_idx, item)

            self.table.resizeColumnsToContents()
            self.status.setText(f"Загружено записей: {len(data)}")

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные: {e}")
            self.status.setText("Ошибка загрузки данных")

    # Открытие диалога добавления квартиры
    def add_flat_dialog(self):
        dialog = AddFlatDialog(self.db, self)
        dialog.exec_()

    def quit_app(self):
        reply = QMessageBox.question(self, 'Выход', 'Вы уверены, что хотите выйти?', QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.db.close()
            self.close()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
