#include <stdlib.h>
#include <check.h>
#include "../src/pghx/logicaldecoding.h"

START_TEST(test_ld_reader_init)
{
    pghx_ld_reader r;
    pghx_ld_reader *pr = &r;
    int res;

    res = pghx_ld_reader_init(pr);

    ck_assert_int_eq(res, 1);
    ck_assert_int_eq(pr->last_status, -1);
    ck_assert_str_eq(pr->slot, "test_slot");

    //free(r);
}
END_TEST

Suite * pghx_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("PGHX");

    /* Core test case */
    tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_ld_reader_init);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(void)
{
    int number_failed;
    Suite *s;
    SRunner *sr;

    s = pghx_suite();
    sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
