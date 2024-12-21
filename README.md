# Spam Cron Smartter 

## Introduction
This is a Node.js application that processes CSV files containing spam, fraud, and TCPA data, tracking their occurrences and maintaining historical records in PostgreSQL. The main logic is implemented in four key JavaScript files: `day.js`, `memory.js`, `downloader.js`, and `tasks.js`.

## Main Logic
- **day.js**: Handles date-related operations and processing of date information from the CSV files.
- **memory.js**: Manages the state and memory of processed records, ensuring efficient tracking and retrieval.
- **downloader.js**: Responsible for downloading and processing the CSV files from specified sources.
- **tasks.js**: Manages scheduled tasks and job processing, ensuring that all necessary operations are executed in a timely manner and handling any asynchronous processes required by the application.

## Features
- **Email Data Extraction**: Automatically extract relevant information from emails.
- **Data Analysis**: Analyze email patterns and trends over time.
- **User-Friendly Interface**: Intuitive UI for easy navigation and usage.
- **Custom Reports**: Generate customized reports based on user preferences.

## Installation
To install this project, follow these steps:
1. Clone the repository: `git clone <repository-url>`
2. Navigate to the project directory: `cd youmail-full-report`
3. Install the required dependencies: `npm install`

## Usage
To use this project, run the following command:
```
forever start tasks.js 
```
This will start the application, and you can follow the on-screen instructions to manage your email data.

This script will run at 2:00AM everyday . to dealwith yesterday data.

ex:
today is 2024/12/19 , the app will be triggered at 2024/12/20 : 02:00:00 . it will download the 2024/12/19 CSV File and do ansylsis and finally update the db and do post meesage to mattermost. 
