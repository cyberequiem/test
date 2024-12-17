#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Объявление переменных для подключения к БД */
exec SQL begin declare section;
    char db_name[50];      /* Имя базы данных */
    char user[50];         /* Логин */
    char password[50];     /* Пароль */
exec SQL end declare section;

void ConnectDB() 
{

      strcpy(db_name, "students"); // Имя базы данных
      strcpy(user, "pmi-b1613"); // Логин
      strcpy(password, "fxIKTS9d$"); // Пароль
      printf("Connecting to db \"%s\"...\n", db_name);
      exec SQL connect to :db_name user :user using :password;
      if (sqlca.sqlcode < 0)
      {
         printf("connect error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
         return;
      }
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Connecting to schema \"pmib1613\"...\n");
      exec sql set search_path to pmib1613;
      if (sqlca.sqlcode < 0)
      {
         printf("connect error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
         return;
      }
      printf("Success! code %d\n", sqlca.sqlcode);
      return;
}

void DisconnectDB()
{
   printf("Disconnecting from db \"%s\"...\n", db_name);
   exec SQL disconnect :db_name;
   if (sqlca.sqlcode < 0)
   {
      printf("disconnect error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      return;
   }
   printf("Success! code %d\n", sqlca.sqlcode);
   return;
}

void PrintMenu()
{
   printf("1) Task1\n");
   printf("2) Task2\n");
   printf("3) Task3\n");
   printf("4) Task4\n");
   printf("5) Task5\n");
   printf("6) Stop the program\n");
}

void Task1()
{
   /*
   1. Выдать число деталей, поставлявшихся для изделий, 
   у которых есть поставки с весом от 5000 до 6000.
   */
   exec sql begin declare section;
      int count; // Результат запроса - число изделий
   exec sql end declare section;
   printf("Starting Task1 request processing...\n");
   exec sql begin work; //начало транзакции
   exec sql select count(distinct spj.n_det) into :count
            from spj
            where spj.n_izd in (
               select spj.n_izd
               from spj
               join p on p.n_det = spj.n_det
               where spj.kol * p.ves between 5000 and 6000
            );
   if (sqlca.sqlcode < 0) //проверка кода возврата запроса
   {
      printf("Task1 error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work; // отмена всех изменений в рамках транзакции
      return;
   }
   else // если успешно завершено
   {
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Count: %d\n", count);
      exec sql commit work; // конец транзакции
      return;
   }
}

void Task2()
{
   /*
   2. Поменять местами вес деталей из Рима и из Парижа, 
   т. е. деталям из Рима установить вес детали из Парижа, 
   а деталям из Парижа установить вес детали из Рима. 
   Если деталей несколько, брать наименьший вес.
   */
   printf("Starting Task2 request processing...\n");
   exec sql begin work; //начало транзакци
   exec sql update p set ves = (
            case 
               when town = 'Рим' then (
                  select min(ves)
                  from p
                  where town = 'Париж')
               else (
                  select min(ves)
                  from p
                  where town='Рим')
            end)
            where town in ('Париж', 'Рим');
   if (sqlca.sqlcode < 0)
   {
      printf("Task2 error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   if (sqlca.sqlcode == 100) //проверка на отсутствие данных
   {
      printf("There is no data to update!\n");
      return;
   }   
   if (sqlca.sqlcode == 0)
   {
      printf("Success! code %d\n", sqlca.sqlcode);
      printf("Changes made: %d\n", sqlca.sqlerrd[2]);
      exec sql commit work; // конец транзакции
      return;
   }
}


void Task3()
{
   /*
   3. Найти детали, имеющие поставки, 
   объем которых не превышает половину максимального объема поставки этой детали поставщиком из Парижа.
   */
   exec sql begin declare section;
      char n_det[6]; // Результат запроса - номера деталей
   exec sql end declare section;
   printf("Starting Task3 request processing...\n");
   // объявление курсора
   exec sql declare curs1 cursor for
      select distinct spj.n_det into :n_det
      from spj 
      join (
         select spj.n_det, max(spj.kol) max_kol
         from spj
         join s on s.n_post = spj.n_post
         where town = 'Париж'
         group by spj.n_det
      ) tab_max_kol on tab_max_kol.n_det = spj.n_det
      where 2 * kol < tab_max_kol.max_kol;
   if (sqlca.sqlcode < 0) // проверка объявления
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //начало транзакци
   exec sql open curs1;   // открываем курсор
   if (sqlca.sqlcode < 0) // проверка открытия
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs1; // следующая строка из активного множества
   if (sqlca.sqlcode < 0) 
   {
      printf("fetch error! %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc); 
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
   if (sqlca.sqlcode == 100)
   {
      printf("No results found\n");
      exec sql commit work;
      return;
   }
   int r_count = 1;
   printf("n_det\n");
   printf("%s\n", n_det);
   while (sqlca.sqlcode == 0) // Пока не дошли до конца активного множества
   {
      exec sql fetch curs1; // следующая строка из активного множества
      if (sqlca.sqlcode == 0)
      {
         printf("%s\n", n_det);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs1; // закрытие курсора
      printf("Success!\n");
      printf("Rows processed: %d\n", r_count);
      exec sql commit work;
      return;
   }
   if (sqlca.sqlcode < 0)
   {
      printf("fetch error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc); 
      exec sql close curs1;
      exec sql rollback work;
      return;
   }
}


void Task4()
{
   /*
   4. Выбрать поставщиков, не поставивших ни одной из деталей, поставляемых для изделий из Парижа.
   */
   exec sql begin declare section;
      char n_post[6]; // Результат запроса - номера поставщиков
   exec sql end declare section;
   printf("Starting Task4 request processing...\n");
   exec sql declare curs2 cursor for
      select n_post into :n_post
      from s
      except
      select n_post
      from spj
      where n_det in (
         select spj.n_det
         from spj
         join j on j.n_izd=spj.n_izd
         where j.town='Париж'
      );
   if (sqlca.sqlcode < 0) // проверка объявления
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //начало транзакци
   exec sql open curs2;   // открываем курсор
   if (sqlca.sqlcode < 0) // проверка открытия
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs2; // следующая строка из активного множества
   if (sqlca.sqlcode < 0) 
   {
      printf("fetch error! %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work; 
      return;
   }
   int r_count = 1;
   printf("n_post\n");
   printf("%s\n", n_post);
   while (sqlca.sqlcode == 0) // Пока не дошли до конца активного множества
   {
      exec sql fetch curs2; // следующая строка из активного множества
      if (sqlca.sqlcode == 0)
      {
         printf("%s\n", n_post);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs2; // закрытие курсора
      printf("Success!\n");
      printf("Rows processed: %d\n", r_count);
      exec sql commit work;
      return;
   }
   if (sqlca.sqlcode < 0)
   {
      printf("fetch error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs2;
      exec sql rollback work; 
      return;
   }
}


void Task5()
{
   /*
   5. Выдать полную информацию о деталях, 
   которые поставлялись ТОЛЬКО поставщиками, проживающими в Афинах.
   */
   exec sql begin declare section;
      char n_det[6], name[20], cvet[20], town[20];
      int ves;
   exec sql end declare section;
   exec sql declare curs3 cursor for
      select p.* into :n_det, :name, :cvet, :ves, :town
      from spj
      join p on p.n_det = spj.n_det
      where spj.n_post in (
         select n_post
         from s
         where town = 'Афины'
      )
      except
      select p.*
      from spj
      join p on p.n_det = spj.n_det
      where spj.n_post not in (
         select n_post
         from s
         where town = 'Афины'
      );
   if (sqlca.sqlcode < 0) // проверка объявления
   {
      printf("declare error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql rollback work;
      return;
   }
   exec sql begin work; //начало транзакци
   exec sql open curs3;   // открываем курсор
   if (sqlca.sqlcode < 0) // проверка открытия
   {
      printf("open error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work;
      return;
   }
   exec sql fetch curs3; // следующая строка из активного множества
   if (sqlca.sqlcode < 0) 
   {
   
      printf("fetch error! %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work; 
      return;
   }
   if (sqlca.sqlcode == 100)
   {
      printf("No results found\n");
      exec sql commit work;
      return;
   }
   int r_count = 1;
   printf("|n_det |name            |cvet         |ves|town          |\n");
   printf("|%.6s|%.20s|%.20s|%d|%.20s|\n", n_det, name, cvet, ves, town);
   while (sqlca.sqlcode == 0) // Пока не дошли до конца активного множества
   {
      exec sql fetch curs3; // следующая строка из активного множества
      if (sqlca.sqlcode == 0)
      {
         printf("|%.6s|%.20s|%.20s|%d|%.20s|\n", n_det, name, cvet, ves, town);
         r_count += 1;
      }
   }
   if (sqlca.sqlcode == 100)
   {
      exec sql close curs3; // закрытие курсора
      printf("Success!\n");
      printf("Rows processed: %d\n", r_count);
      exec sql commit work;
      return;
   }
   if (sqlca.sqlcode < 0)
   {
      printf("fetch error! code %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
      exec sql close curs3;
      exec sql rollback work; 
      return;
   }
}

int main()
{
   ConnectDB();
   while(true)
   {
      printf("What to do?\n");
      PrintMenu();
      printf("Choose the number: ");
      int number = 0;
      scanf("%d", &number);
      switch (number)
      {
         case 1:
            Task1();
            break;
         case 2:
            Task2();
            break;
         case 3:
            Task3();
            break;
         case 4:
            Task4();
            break;
         case 5:
            Task5();
            break;
         case 6:
            DisconnectDB();
            return 0;
         default:
            printf("Try again!\n");
            return 0;
         break;
      }
   }
}
