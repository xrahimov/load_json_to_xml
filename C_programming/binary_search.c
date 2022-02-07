
#include <stdio.h>

int binary_search(int arr[], int el, int l, int r);

int main()
{
    int us[] = {4, 6, 3, 1, 7, 9, 10, 2, 5, 8};
    int s[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int index = binary_search(s, 12, 0, 10);
    printf("index of element: %d\n", index);
    return 0;
}

int binary_search(int arr[], int el, int l, int r)
{
    int mid = l + (r - l) / 2;
    if (l > r)
        return -1;
    if (arr[mid] == el)
        return mid;
    else if (arr[mid] > el)
        return binary_search(arr, el, l, mid - 1);
    else
        return binary_search(arr, el, mid + 1, r);
}