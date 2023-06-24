#include "bpt.h"
#include "buffer_manager.h"

// MAIN

//FILE* fd[11];
//int used_fd[11], page_size[11];
//char* table_name[11];
//

int Insert[11], Delete[11], Find[11];
int Ins_s[11], Del_s[11], Fin_s[11], Fin_m[11];
int main( int argc, char ** argv ) {
    
    char * input_file;
    FILE * fp;
//    node * root;
    int input, range2;
    char instruction[10], file_path[1000];
    char license_part;

    freopen("input.txt", "r", stdin);
    
//    root = NULL;
    verbose_output = false;
    
    if (argc > 1) {
        order = atoi(argv[1]);
        if (order < MIN_ORDER || order > MAX_ORDER) {
            fprintf(stderr, "Invalid order: %d .\n\n", order);
//            usage_3();
            exit(EXIT_FAILURE);
        }
    }

//    license_notice();
//    usage_1();
//    usage_2();

//    if (argc > 2) {
//        input_file = argv[2];
//        fp = fopen(input_file, "r");
//        if (fp == NULL) {
//            perror("Failure  open input file.");
//            exit(EXIT_FAILURE);
//        }
//        while (!feof(fp)) {
//            fscanf(fp, "%d\n", &input);
//            root = insert(root, input, input);
//        }
//        fclose(fp);
//        print_tree(root);
//    }
    
//    printf("> ");

    char value[120], findck[120];
    int64_t key, cnt = 0;
    int ck, buf, table_id;
    init_db(100);
    while (scanf("%s", instruction) != EOF) {
//        if (!strcmp(instruction, "ck"))
//        {
//            print_tree();
//        }
        if (!strcmp(instruction, "x"))
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
            cnt++;
            Insert[table_id]++;
            if (!db_insert(table_id, key,value))
            {
                if (ftell(fd[table_id])%4096)
                {
                    printf("error %lld-th insert\n", cnt);
                    return 0;
                }
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
            cnt++;
            Find[table_id]++;
            if (!db_find(table_id, key, value))
            {
                Fin_s[table_id]++;
                change(key, findck);
                if (strcmp(value + 4, findck)) {
                    puts("error");
                    printf("%lld\n%s\n%s\n\n", key, value+4, findck);
                    return 0;
                }
//                puts(value);
            } else
            {
//                puts("record not found");
            }
        } else if (!strcmp(instruction, "3"))
        {
//            if (Delete % 100000 == 0)
//            {
//                if (print_leaves() != Ins_s - Del_s)
//                {
//                    printf("cnt : %lld\n", cnt);
//                    puts("error");
//                    return 0;
//                }
//            }
            scanf("%d %lld",&table_id, &key);
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
        } else if(!strcmp(instruction, "4"))
        {
            scanf("%d", &table_id);
            close_table(table_id);
        }else if (!strcmp(instruction, "quit"))
        {
            if (shutdown_db() == -1)
                puts("shutdown fail");
            else
                puts("shutdown success");
            return 0;
        }

//        switch (instruction) {
//            case 'd':
//                scanf("%d", &input);
//                root = delete(root, input);
//                print_tree(root);
//                break;
//            case 'i':
//                scanf("%d", &input);
//                root = insert(root, input, input);
//                print_tree(root);
//                break;
//            case 'f':
//            case 'p':
//                scanf("%d", &input);
//                find_and_print(root, input, instruction == 'p');
//                break;
//            case 'r':
//                scanf("%d %d", &input, &range2);
//                if (input > range2) {
//                    int tmp = range2;
//                    range2 = input;
//                    input = tmp;
//                }
//                find_and_print_range(root, input, range2, instruction == 'p');
//                break;
//            case 'l':
//                print_leaves(root);
//                break;
//            case 'q':
//                while (getchar() != (int)'\n');
//                return EXIT_SUCCESS;
//                break;
//            case 't':
//                print_tree(root);
//                break;
//            case 'v':
//                verbose_output = !verbose_output;
//                break;
//            case 'x':
//                if (root)
//                    root = destroy_tree(root);
//                print_tree(root);
//                break;
//            case 'o':
//
//                break;
//            default:
//                usage_2();
//                break;
//        }
        
        while (getchar() != (int)'\n');
        if (cnt%10000 == 0)
            printf("cnt : %d\n", cnt);
//        printf("> ");
//        if (cnt%100000 == 0)
//        {
//            printf("Cnt : %lld\nInsert : %d, suc : %d\nDelete : %d, suc : %d\nFind : %d, suc : %d, not match : %d\n\n", cnt, Insert, Ins_s, Delete, Del_s, Find, Fin_s, Fin_m);
//        }
    }
//    printf("\n");
    int SUC = 0, FIL = 0;
    for (int i = 1; i <= 5; ++i) {
        printf("table : %d\nInsert : %d, suc : %d\nDelete : %d, suc : %d\nFind : %d, suc : %d\n\n", i, Insert[i], Ins_s[i], Delete[i], Del_s[i], Find[i], Fin_s[i], Fin_m[i]);
        SUC += Ins_s[i] + Del_s[i] + Fin_s[i];
    }
    FIL = cnt - SUC;
    printf("suc : %d\nfil : %d\n", SUC, FIL);
    return EXIT_SUCCESS;
}
