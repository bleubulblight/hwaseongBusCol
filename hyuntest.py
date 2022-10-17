def box(boxcontents, boxindex) :
    print (box_in_box1() + str(box_in_box2(boxcontents, boxindex)))

def box_in_box1() :
    return "box"
    
def box_in_box2(boxcontents, boxindex) :
    boxindex += 1
    return boxcontents[boxindex], boxindex

def main():
    boxcontents = ["1","2"]
    boxindex = 1
    text, boxindex = box(boxcontents, boxindex)
    
if __name__ == "__main__":
    main()