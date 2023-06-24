#include "bpt.h"

int Insert[11], Delete[11], Find[11];
int Ins_s[11], Del_s[11], Fin_s[11], Fin_m[11];
int main( int argc, char ** argv ) {
    clock_t st, ed;
    char instruction[10], file_path[1000];

//    freopen("join_table.txt", "r", stdin);
    
//    verbose_output = false;

    char value[120], findck[120];
    int64_t key, cnt = 0;
    int ck, buf, table_id;
    st = clock();
    init_db(500);
    while (scanf("%s", instruction) != EOF) {
        if (!strcmp(instruction,"5"))
        {
            int table1, table2;
            scanf("%d %d %s", &table1, &table2, file_path);
            puts("into join");
            join_table(table1, table2, file_path);
            puts("join success");
        } else if (!strcmp(instruction, "x"))
        {
            scanf("%d", &buf);
            init_db(buf);
            puts("buffer init");
        }else if(!strcmp(instruction,"0"))
        {
            scanf("%*c%[^\n]",file_path);
            ck = open_table(file_path);
            if (ck < 0) {
                perror("Failure open table");
                exit(EXIT_FAILURE);
            }
            printf("%s load\n",file_path);
        } else if (!strcmp(instruction,"1"))
        {
            scanf("%d %lld", &table_id, &key);
            scanf("%*c%[^\n]",value);
//            if (table_id != 1) continue;
//            if ((temp = print_leaves(table_id)) != Ins_s[table_id] - Del_s[table_id])
//            {
//                printf("cnt : %lld, Insert : %d, suc : %d, Delete : %d, suc : %d\n", cnt, Insert[table_id], Ins_s[table_id], Delete[table_id], Del_s[table_id]);
//                printf("Isuc - Dsuc = %d, now_leaves : %d, prev_leaves : %d\n", Ins_s[table_id] - Del_s[table_id], temp, prev);
//                puts("error");
//                return 0;
//            }
//            prev = temp;
            cnt++;
//            if (cnt == 17573)
//            {
//                temp = 0;
//            }
            Insert[table_id]++;
            if (!db_insert(table_id, key,value))
            {
//                if (ftell(fd[table_id])%4096)
//                {
//                    printf("error %lld-th insert\n", cnt);
//                    return 0;
//                }
//                puts("insert success");
//                print_tree(table_id);
                Ins_s[table_id]++;
            } else
            {
//                puts("insert fail");
            }
        } else if (!strcmp(instruction,"2"))
        {
//            scanf("%lld %s", &key, value);
            scanf("%d %lld", &table_id, &key);
//            if (table_id != 1) continue;
            cnt++;
            Find[table_id]++;
            if (!db_find(table_id, key, value))
            {
                Fin_s[table_id]++;
//                change(key, findck);
//                if (strcmp(value + 1, findck)) {
//                    puts("error");
//                    printf("%lld\n%s\n%s\n\n", key, value+1, findck);
//                    return 0;
//                }
//                puts(value);
            } else
            {
//                puts("record not found");
            }
        } else if (!strcmp(instruction, "3"))
        {
            scanf("%d %lld",&table_id, &key);
//            if (table_id != 1) continue;
//            if (print_leaves(table_id) != Ins_s[table_id] - Del_s[table_id])
//            {
//                printf("cnt : %lld, Delete : %d, suc : %d\n", cnt, Delete[table_id], Del_s[table_id]);
//                puts("error");
//                return 0;
//            }
            cnt++;
            Delete[table_id]++;
//            if (print_leaves(table_id) == -1)
//            {
//                puts("error in del");
//                return 0;
//            }
            if (!db_delete(table_id, key))
            {
                Del_s[table_id]++;
//                puts("delete success");
//                print_tree(table_id);
//                page_t* temp = (page_t*)malloc(sizeof(page_t));
//                buffer_read_page(table_id,0, temp);
//                pagenum_t rootnum = get_root_pagenum(temp);
//                if (rootnum == 0)
//                {
//                    printf("table_%d is empty\n", table_id);
//                }
//                free(temp);
            } else
            {
//                puts("delete fail");
            }
//            print_tree(table_id);
        } else if(!strcmp(instruction, "4"))
        {
            printf("pin : %d\n", buffer_pin_check());
            scanf("%d", &table_id);
            if (close_table(table_id) == 0)
                printf("table %d is close\n", table_id);
            else
                printf("table %d close fail\n", table_id);
        }else if (!strcmp(instruction, "quit"))
        {
            printf("pin : %d\n", buffer_pin_check());
            if (shutdown_db() == -1)
                puts("shutdown fail");
            else
                puts("shutdown success");
            break;
        }
        
        while (getchar() != (int)'\n');
        if (cnt%100000 == 0)
            printf("cnt : %lld\n", cnt);
//        printf("> ");
//        if (cnt%100000 == 0)
//        {
//            printf("Cnt : %lld\nInsert : %d, suc : %d\nDelete : %d, suc : %d\nFind : %d, suc : %d, not match : %d\n\n", cnt, Insert, Ins_s, Delete, Del_s, Find, Fin_s, Fin_m);
//        }
    }
//    printf("\n");
    ed = clock();
    printf("time : %lf\n", (double)(ed - st)/1000.0);
    int SUC = 0, FIL = 0;
    for (int i = 1; i <= 5; ++i) {
        printf("table : %d\nInsert : %d, suc : %d\nDelete : %d, suc : %d\nFind : %d, suc : %d\n\n", i, Insert[i], Ins_s[i], Delete[i], Del_s[i], Find[i], Fin_s[i]);
        SUC += Ins_s[i] + Del_s[i] + Fin_s[i];
    }
    FIL = cnt - SUC;
    printf("suc : %d\nfil : %d\n", SUC, FIL);
    return EXIT_SUCCESS;
}