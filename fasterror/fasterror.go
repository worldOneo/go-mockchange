package fasterror

type Error struct {
	data    string
	present bool
}

func (fastError *Error) ToString() string {
	return fastError.data
}

func Create(text string) Error {
	var fastError Error
	fastError.data = text
	fastError.present = true
	return fastError
}

func Nil() Error {
	return Error{present: false}
}

func (fastError *Error) IsNil() bool {
	return !fastError.present
}