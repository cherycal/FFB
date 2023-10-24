import push

inst = push.Push()

msgs = ("One", "Two", "Three","Fourfsdfsdfsdfsdfsdfsdfsdfsdf fsdfsdfsdf sd", "Five: dfsdfsdfdsfhksdfhskdf", \
        "Six: dflsdjflsdjfldfjlsdfj", "Seven: dskfjsdlfsdlfjdslfjsdlfjsdlfjsdlfjs" , "Eight: dsfsdjfhsdkfhksdffsdf", \
        "Nine: dlskfjdslkfjsdlfkjsdlfjsdlkfjsld", "Ten: dsldjfsdlfjdslfjsdlfjsdlfsdlfjsdlfdsdsfj", \
        "Eleven: dsfsdfdsfjsdlfkjsdlkfjsdlfjsldfjsdlfjsdffffffsdffds", "Twelve: dsfdsfkdsjfldsjflksjdfljsdlfkjdslfk", \
        "Thriteen: dsadksfksdlfjdsklfjdslkfjsldkfjlsdfjlsdkfjsldkfjlsdkfjsldjf", "Fourteen: ddlkjdflsdfjsldfjldsfjsld")

inst.push_list(msgs, "Test push list")
