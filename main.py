import os
import pandas as pd
from io import BytesIO
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from dotenv import load_dotenv
from sqlalchemy import create_engine, select, distinct, Table, MetaData, Column
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.types import String

# Load environment variables from .env file
load_dotenv()

# SQLAlchemy setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
metadata = MetaData()


# Define ORM mapping for products_ozon table
class ProductOzon(Base):
    __tablename__ = 'products_ozon'
    Артикул = Column(String, primary_key=True)
    Barcode = Column(String)


# Function to process the Excel file and create filtered dataframes
def process_excel(file_path):
    df = pd.read_excel(file_path)

    # Query the database for distinct Артикул and Barcode
    products_table = Table('products_ozon', metadata, autoload_with=engine)
    query = select(distinct(products_table.c['Артикул']), products_table.c['Barcode'].label('Штрихкод EAN13'))
    result = session.execute(query)
    db_df = pd.DataFrame(result.fetchall(), columns=['Артикул', 'Штрихкод EAN13'])
    db_df = db_df.drop_duplicates(subset=['Штрихкод EAN13'])
    db_df['Артикул'] = db_df['Артикул'].str.replace('\'', '')
    db_df['Штрихкод EAN13'] = db_df['Штрихкод EAN13'].str.replace('.0', '')

    # Merge the dataframes
    df = pd.merge(df, db_df, on='Артикул', how='left')

    # Add ПРЕДМЕТ column with NaN values
    df['ПРЕДМЕТ'] = pd.NA

    # remane column За продажу или возврат до вычета комиссий и услуг to СУмма
    df = df.rename(columns={"За продажу или возврат до вычета комиссий и услуг": "Сумма","SKU": "Код"})
    # Filter dataframes
    otgruzka_df = df[(df["Тип начисления"] == "Доставка покупателю") & (df['Количество'] > 0)]
    vozvrat_df = df[(df["Тип начисления"] == "Получение возврата, отмены, невыкупа от покупателя") & (df['Количество'] > 0)]


    # Select only the required columns
    otgruzka_df = otgruzka_df[["Штрихкод EAN13", "Артикул", "Код", "ПРЕДМЕТ", "Сумма", "Количество"]]
    vozvrat_df = vozvrat_df[["Штрихкод EAN13", "Артикул", "Код", "ПРЕДМЕТ", "Сумма", "Количество"]]



    # Group by Артикул and SKU otgruzka_df
    otgruzka_df = otgruzka_df.groupby(["Штрихкод EAN13", 'Артикул', 'Код']).agg({
        'Сумма': 'sum',
        'Количество': 'sum'
    }).reset_index()


    # Group by Артикул and SKU vozvrat_df
    vozvrat_df = vozvrat_df.groupby(["Штрихкод EAN13", 'Артикул', 'Код']).agg({
        'Сумма': 'sum',
        'Количество': 'sum'
    }).reset_index()

    # Add ЦЕНА columns
    otgruzka_df["ЦЕНА"] = otgruzka_df["Сумма"] / otgruzka_df["Количество"]
    vozvrat_df["ЦЕНА: Цена продажи"] = vozvrat_df["Сумма"] / vozvrat_df["Количество"]





    return otgruzka_df, vozvrat_df


# Start command handler
async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("Привет! Пожалуйста, отправьте мне файл Excel для обработки.")


# File handler
async def handle_file(update: Update, context: CallbackContext) -> None:
    file = await update.message.document.get_file()
    file_path = os.path.join(os.getcwd(), 'received_file.xlsx')
    await file.download_to_drive(file_path)

    # Process the Excel file
    otgruzka_df, vozvrat_df = process_excel(file_path)

    # Save the filtered data to new Excel files
    otgruzka_output = BytesIO()
    vozvrat_output = BytesIO()
    otgruzka_df.to_excel(otgruzka_output, index=False)
    vozvrat_df.to_excel(vozvrat_output, index=False)
    otgruzka_output.seek(0)
    vozvrat_output.seek(0)

    # Send the filtered files back to the user
    await update.message.reply_document(document=InputFile(otgruzka_output, filename="Отгрузка.xlsx"))
    await update.message.reply_document(document=InputFile(vozvrat_output, filename="Возврат.xlsx"))


# Main function to set up the bot
def main():
    # Load token from environment variable
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        raise ValueError("No TELEGRAM_BOT_TOKEN found in environment variables")

    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        MessageHandler(filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                       handle_file))

    application.run_polling()


if __name__ == '__main__':
    main()
