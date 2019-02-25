from PIL import ImageGrab,Image
import pytesseract
import tkinter
import  threading
from pynput import mouse,keyboard
# 提前绑定鼠标位置事件
global old_x,old_y,new_x,new_y
global menu,win,t
old_x,old_y,new_x,new_y =0,0,0,0
def createMenu():
    global menu,t
    menu = tkinter.Tk()
    btn=tkinter.Button(menu,text='开始截图',width=13,height=2,command=createIframe)
    clear = tkinter.Button(menu,text='清空',width=13,height=2,command=clearIframe)
    btn.pack()
    clear.pack()
    t = tkinter.Text(menu)
    t.pack()
    menu.mainloop()
def clearIframe():
    global t
    t.delete('1.0','end')
def createIframe():
    global win
    win = tkinter.Toplevel()
    win.overrideredirect(True)
    win.attributes("-alpha",0.1)
    w = win.winfo_screenwidth()
    h = win.winfo_screenheight()
    win.geometry("%dx%d" % (w, h))
    t = threading.Thread(target=loop, name='LoopThread')
    t.start()

def loop():
    listenerMouse()
    print(old_x, old_y, new_x, new_y)
    if old_x < new_x:
        sortCut(old_x, old_y, new_x, new_y)
    else:
        sortCut(new_x, new_y, old_x, old_y)
    ser()
def on_click(x,y,button,pressed):
    global old_x, old_y, new_x, new_y
    if pressed:
        if x !=0:
           old_x=x
        if y !=0:
           old_y=y
    else:
        if x !=0:
           new_x = x
        if y !=0:
           new_y = y

    if not pressed:
        # Stop listener
        return False

def listenerMouse():
    with mouse.Listener(on_click=on_click) as listener:
         listener.join()

def sortCut(old_x, old_y, new_x, new_y):
    bbox = (old_x, old_y, new_x, new_y)
    im = ImageGrab.grab(bbox)
    # 参数 保存截图文件的路径
    im.save('as.png')
def ser():
    pytesseract.pytesseract.tesseract_cmd = 'C://Program Files (x86)/Tesseract/tesseract.exe'
    text = pytesseract.image_to_string(Image.open('as.png'))
    print(text)
    global t
    t.insert('insert',text)
    win.destroy()
if __name__ == '__main__':
    createMenu()