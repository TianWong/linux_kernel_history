git clone git://git.kernel.org/pub/scm/linux/kernel/git/stable/linux-stable.git

cd linux-stable

git log --date=iso --pretty=format:"%at%x09%ad%x09%H%x09%an%x09%ae%x09%s" --stat --no-merges > ../raw.revlog

# use the below line if you exceed diff.renameLimit
# git config diff.renameLimit 999999

cd ..

python3 process.py